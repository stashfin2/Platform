import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { ExecuteStatementCommand, DescribeStatementCommand, GetStatementResultCommand } from '@aws-sdk/client-redshift-data';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { RedshiftClientFactory } from '../config/redshift.config';
import { CleverTapS3ClientFactory } from '../config/clevertap.config';
import { LoggerService } from './logger.service';
import * as zlib from 'zlib';
import * as readline from 'readline';

/**
 * Schema Sync Service
 * Dynamically adds missing columns to Redshift table based on CSV headers
 */
@Service()
export class SchemaSyncService {
  constructor(
    @Inject() private readonly redshiftClientFactory: RedshiftClientFactory,
    @Inject() private readonly s3ClientFactory: CleverTapS3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * Get CSV headers from first file in S3
   */
  async getCsvHeaders(csvFile: string): Promise<string[]> {
    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      this.logger.info('📥 Downloading sample CSV to inspect headers', { csvFile });

      const getCommand = new GetObjectCommand({
        Bucket: config.bucket,
        Key: csvFile,
      });

      const response = await client.send(getCommand);
      let stream = response.Body as any;

      // Decompress if gzipped
      if (csvFile.endsWith('.gz')) {
        stream = stream.pipe(zlib.createGunzip());
      }

      return new Promise((resolve, reject) => {
        const rl = readline.createInterface({
          input: stream,
          crlfDelay: Infinity,
        });

        let headers: string[] = [];

        rl.on('line', (line) => {
          // Parse headers from first line
          headers = this.parseCsvLine(line);
          rl.close();
        });

        rl.on('close', () => {
          resolve(headers);
        });

        rl.on('error', (error) => {
          reject(error);
        });
      });
    } catch (error) {
      this.logger.error('❌ Error getting CSV headers', error);
      throw error;
    }
  }

  /**
   * Get existing table columns from Redshift
   */
  async getTableColumns(tableName: string): Promise<string[]> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();

    const [schema, table] = tableName.includes('.') 
      ? tableName.split('.') 
      : ['public', tableName];

    const sql = `
      SELECT column_name
      FROM information_schema.columns
      WHERE table_schema = '${schema}'
        AND table_name = '${table}'
      ORDER BY ordinal_position
    `;

    try {
      const command = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: sql,
      });

      const response = await client.send(command);
      await this.waitForQuery(response.Id!);

      const getResultCommand = new GetStatementResultCommand({ Id: response.Id });
      const result = await client.send(getResultCommand);

      const columns = result.Records?.map(record => record[0].stringValue!) || [];
      
      this.logger.info('📊 Existing table columns', {
        tableName,
        columnCount: columns.length,
      });

      return columns;
    } catch (error) {
      this.logger.error('❌ Error getting table columns', error);
      throw error;
    }
  }

  /**
   * Add missing columns to table
   */
  async addMissingColumns(tableName: string, csvHeaders: string[], existingColumns: string[]): Promise<number> {
    // Normalize column names for comparison (lowercase)
    const existingColumnsLower = existingColumns.map(c => c.toLowerCase());
    const csvHeadersLower = csvHeaders.map(c => c.toLowerCase());

    // Find missing columns
    const missingColumns = csvHeaders.filter((_, index) => 
      !existingColumnsLower.includes(csvHeadersLower[index])
    );

    if (missingColumns.length === 0) {
      this.logger.info('✅ No missing columns - schema is up to date');
      return 0;
    }

    this.logger.info('🔧 Adding missing columns to table', {
      tableName,
      missingCount: missingColumns.length,
    });

    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();

    // Add columns one at a time (Redshift doesn't support multiple ADD COLUMN in one ALTER)
    let totalAdded = 0;

    for (let i = 0; i < missingColumns.length; i++) {
      const column = missingColumns[i];
      
      // Sanitize column name for SQL
      // Replace special chars with underscores, ensure starts with letter or underscore
      let safeColumnName = column
        .replace(/[^a-zA-Z0-9_]/g, '_')
        .replace(/^[0-9]/, '_$&') // Prefix numbers with underscore
        .replace(/_+/g, '_') // Collapse multiple underscores
        .toLowerCase();
      
      // Skip if empty after sanitization
      if (!safeColumnName || safeColumnName === '_') {
        this.logger.warn(`Skipping invalid column name: "${column}"`);
        continue;
      }

      const sql = `ALTER TABLE ${tableName} ADD COLUMN ${safeColumnName} VARCHAR(65535);`;

      try {
        if ((i + 1) % 25 === 0) {
          this.logger.info(`Adding column ${i + 1}/${missingColumns.length}...`);
        }

        const command = new ExecuteStatementCommand({
          ClusterIdentifier: config.clusterIdentifier,
          Database: config.database,
          DbUser: config.dbUser,
          Sql: sql,
        });

        const response = await client.send(command);
        await this.waitForQuery(response.Id!);

        totalAdded++;
      } catch (error: any) {
        // Skip if column already exists
        if (error.message?.includes('already exists')) {
          this.logger.info(`⚠️  Column ${safeColumnName} already exists, skipping`);
          totalAdded++;
        } else {
          this.logger.error(`❌ Error adding column: ${safeColumnName}`, error);
          throw error;
        }
      }
    }

    this.logger.info('✅ All missing columns added', {
      tableName,
      columnsAdded: totalAdded,
    });

    return totalAdded;
  }

  /**
   * Sync schema: inspect CSV, compare with table, add missing columns
   */
  async syncSchema(tableName: string, sampleCsvFile: string): Promise<void> {
    this.logger.info('🔄 Starting schema sync', { tableName, sampleCsvFile });

    // 1. Get CSV headers
    const csvHeaders = await this.getCsvHeaders(sampleCsvFile);
    this.logger.info(`📋 CSV has ${csvHeaders.length} columns`);

    // 2. Get existing table columns
    const existingColumns = await this.getTableColumns(tableName);
    this.logger.info(`📊 Table has ${existingColumns.length} columns`);

    // 3. Add missing columns
    const columnsAdded = await this.addMissingColumns(tableName, csvHeaders, existingColumns);

    if (columnsAdded > 0) {
      this.logger.info(`✅ Schema sync complete - added ${columnsAdded} new columns`);
    } else {
      this.logger.info('✅ Schema sync complete - no changes needed');
    }
  }

  /**
   * Wait for Redshift query to complete
   */
  private async waitForQuery(queryId: string): Promise<void> {
    const client = this.redshiftClientFactory.getClient();

    for (let i = 0; i < 60; i++) {
      await new Promise(resolve => setTimeout(resolve, 1000));
      
      const describeCommand = new DescribeStatementCommand({ Id: queryId });
      const response = await client.send(describeCommand);

      if (response.Status === 'FINISHED') {
        return;
      } else if (response.Status === 'FAILED' || response.Status === 'ABORTED') {
        throw new Error(`Query failed: ${response.Error}`);
      }
    }

    throw new Error('Query timeout');
  }

  /**
   * Parse CSV line handling quoted fields
   */
  private parseCsvLine(line: string): string[] {
    const values: string[] = [];
    let currentValue = '';
    let insideQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];

      if (char === '"') {
        if (insideQuotes && nextChar === '"') {
          currentValue += '"';
          i++;
        } else {
          insideQuotes = !insideQuotes;
        }
      } else if (char === ',' && !insideQuotes) {
        values.push(currentValue.trim());
        currentValue = '';
      } else {
        currentValue += char;
      }
    }

    values.push(currentValue.trim());
    return values;
  }
}

