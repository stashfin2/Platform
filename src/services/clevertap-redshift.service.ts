import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { ExecuteStatementCommand, DescribeStatementCommand } from '@aws-sdk/client-redshift-data';
import { PutObjectCommand, DeleteObjectCommand } from '@aws-sdk/client-s3';
import { RedshiftClientFactory } from '../config/redshift.config';
import { CleverTapJsonS3ClientFactory } from '../config/clevertap-json-s3.config';
import { LoggerService } from './logger.service';

export interface CleverTapEvent {
  event_date: string;
  event_ts: string;
  user_id: string | null;
  event_name: string;
  source_file: string;
  attributes: any;
}

/**
 * CleverTap Redshift Service
 * Handles inserting CleverTap events into Redshift with the specific schema
 */
@Service()
export class CleverTapRedshiftService {
  private readonly tableName: string;
  private readonly useCopyCommand: boolean;
  private readonly jsonPrefix: string;

  constructor(
    @Inject() private readonly redshiftClientFactory: RedshiftClientFactory,
    @Inject() private readonly s3ClientFactory: CleverTapJsonS3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {
    this.tableName = process.env.CLEVERTAP_TABLE_NAME || 'appsflyer.clevertap_events';
    // Use COPY command by default (much faster than INSERT)
    this.useCopyCommand = process.env.CLEVERTAP_USE_COPY_COMMAND !== 'false';
    this.jsonPrefix = process.env.CLEVERTAP_JSON_PREFIX || 'clevertap-json-temp';
  }

  /**
   * Batch insert CleverTap events into Redshift
   * Uses COPY command (faster) or INSERT (fallback)
   * Waits for statement completion before returning
   * Returns the number of rows inserted
   */
  async insertBatchData(events: CleverTapEvent[]): Promise<number> {
    if (events.length === 0) {
      throw new Error('Cannot insert empty batch');
    }

    if (this.useCopyCommand) {
      return this.insertBatchDataWithCopy(events);
    } else {
      return this.insertBatchDataWithInsert(events);
    }
  }

  /**
   * Insert using COPY command (faster - recommended)
   * Writes batch to S3 temporarily, then COPYs from S3
   */
  private async insertBatchDataWithCopy(events: CleverTapEvent[]): Promise<number> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();
    const s3Client = this.s3ClientFactory.getClient();
    const s3Config = this.s3ClientFactory.getConfig();

    // Generate unique S3 key for this batch
    const batchId = `${Date.now()}_${Math.random().toString(36).substring(7)}`;
    const s3Key = `${this.jsonPrefix}/batch_${batchId}.json`;

    try {
      // Convert events to newline-delimited JSON
      const jsonLines = events.map(event => JSON.stringify(event));
      const jsonContent = jsonLines.join('\n') + '\n';

      // Upload JSON to S3
      this.logger.debug('Uploading batch to S3 for COPY command', {
        s3Key,
        rowCount: events.length,
        sizeKB: Math.floor(Buffer.byteLength(jsonContent, 'utf8') / 1024),
      });

      const putCommand = new PutObjectCommand({
        Bucket: s3Config.bucket,
        Key: s3Key,
        Body: jsonContent,
        ContentType: 'application/json',
      });

      await s3Client.send(putCommand);

      this.logger.info('📤 Uploaded batch to S3', {
        s3Key,
        rowCount: events.length,
      });

      // Build COPY command
      const sql = s3Config.accessKeyId && s3Config.secretAccessKey
        ? `
          COPY ${this.tableName}
          FROM 's3://${s3Config.bucket}/${s3Key}'
          ACCESS_KEY_ID '${s3Config.accessKeyId}'
          SECRET_ACCESS_KEY '${s3Config.secretAccessKey}'
          JSON 'auto'
          TIMEFORMAT 'auto'
          DATEFORMAT 'auto'
          TRUNCATECOLUMNS
          BLANKSASNULL
          EMPTYASNULL
          MAXERROR 100
          ACCEPTINVCHARS
          COMPUPDATE OFF
          STATUPDATE OFF;
        `
        : `
          COPY ${this.tableName}
          FROM 's3://${s3Config.bucket}/${s3Key}'
          IAM_ROLE '${process.env.REDSHIFT_IAM_ROLE || ''}'
          JSON 'auto'
          TIMEFORMAT 'auto'
          DATEFORMAT 'auto'
          TRUNCATECOLUMNS
          BLANKSASNULL
          EMPTYASNULL
          MAXERROR 100
          ACCEPTINVCHARS
          COMPUPDATE OFF
          STATUPDATE OFF;
        `;

      this.logger.info('📥 Executing COPY command for CleverTap events', {
        s3Path: `s3://${s3Config.bucket}/${s3Key}`,
        rowCount: events.length,
        tableName: this.tableName,
      });

      const command = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: sql,
      });

      const response = await client.send(command);
      const statementId = response.Id || '';

      // Wait for COPY to complete
      const rowsInserted = await this.waitForStatementCompletion(statementId);

      // Clean up S3 file after successful COPY
      try {
        const deleteCommand = new DeleteObjectCommand({
          Bucket: s3Config.bucket,
          Key: s3Key,
        });
        await s3Client.send(deleteCommand);
        this.logger.debug('🗑️  Cleaned up temporary S3 file', { s3Key });
      } catch (cleanupError) {
        // Log but don't fail - cleanup is best effort
        this.logger.warn('⚠️  Failed to clean up S3 file (non-critical)', {
          s3Key,
          error: cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
        });
      }

      this.logger.info('✅ COPY completed successfully', {
        statementId,
        rowCount: events.length,
        rowsInserted,
        tableName: this.tableName,
        s3Key,
        note: 'Redshift Data API often returns rowsAffected=0 for COPY. Actual rows loaded may differ.',
      });

      // For COPY commands, Redshift Data API returns rowsAffected=0 even when successful
      // Return the expected row count since COPY completed without errors
      return rowsInserted > 0 ? rowsInserted : events.length;
    } catch (error) {
      // Try to clean up S3 file on error
      try {
        const deleteCommand = new DeleteObjectCommand({
          Bucket: s3Config.bucket,
          Key: s3Key,
        });
        await s3Client.send(deleteCommand);
      } catch (cleanupError) {
        // Ignore cleanup errors
      }

      this.logger.error('❌ Error executing COPY command', error, {
        rowCount: events.length,
        tableName: this.tableName,
        s3Key,
      });
      throw error;
    }
  }

  /**
   * Insert using INSERT statement (fallback)
   */
  private async insertBatchDataWithInsert(events: CleverTapEvent[]): Promise<number> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();

    try {
      // Build batch INSERT statement
      const values = events.map(event => {
        const eventDate = this.formatDate(event.event_date);
        const eventTs = this.formatTimestamp(event.event_ts);
        const userId = this.formatValue(event.user_id);
        const eventName = this.formatValue(event.event_name);
        const sourceFile = this.formatValue(event.source_file);
        const attributes = this.formatSuperValue(event.attributes);

        return `(${eventDate}, ${eventTs}, ${userId}, ${eventName}, ${sourceFile}, ${attributes})`;
      }).join(',\n        ');

      const sql = `
        INSERT INTO ${this.tableName} (
          event_date,
          event_ts,
          user_id,
          event_name,
          source_file,
          attributes
        ) VALUES
        ${values};
      `;

      this.logger.debug('Executing batch INSERT for CleverTap events', {
        rowCount: events.length,
        tableName: this.tableName,
      });

      const command = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: sql,
      });

      const response = await client.send(command);
      const statementId = response.Id || '';

      this.logger.info('📨 CleverTap INSERT statement submitted', {
        statementId,
        rowCount: events.length,
        tableName: this.tableName,
      });

      // Wait for statement to complete
      const rowsInserted = await this.waitForStatementCompletion(statementId);

      this.logger.info('✅ Batch inserted CleverTap events into Redshift', {
        statementId,
        rowCount: events.length,
        rowsInserted,
        tableName: this.tableName,
      });

      return rowsInserted;
    } catch (error) {
      this.logger.error('❌ Error inserting CleverTap batch data into Redshift', error, {
        rowCount: events.length,
        tableName: this.tableName,
      });
      throw error;
    }
  }

  /**
   * Wait for Redshift statement to complete
   * Returns the number of rows affected
   */
  private async waitForStatementCompletion(statementId: string): Promise<number> {
    const client = this.redshiftClientFactory.getClient();
    const startTime = Date.now();
    const maxRetries = 180; // 6 minutes (180 * 2 seconds)

    for (let i = 0; i < maxRetries; i++) {
      try {
        const command = new DescribeStatementCommand({ Id: statementId });
        const response = await client.send(command);
        
        const status = response.Status;
        
        if (i > 0 && i % 10 === 0) {
          // Log progress every 10 attempts (every 20 seconds)
          this.logger.info(`⏳ Waiting for CleverTap INSERT to complete...`, {
            statementId,
            status,
            attempt: i + 1,
            maxRetries,
            elapsedSeconds: Math.floor((Date.now() - startTime) / 1000),
          });
        }
        
        if (status === 'FINISHED') {
          const duration = Date.now() - startTime;
          const rowsAffected = response.ResultRows || 0;
          this.logger.info('✅ CleverTap INSERT completed successfully', {
            statementId,
            durationMs: duration,
            durationSeconds: Math.floor(duration / 1000),
            attempts: i + 1,
            rowsAffected,
          });
          return rowsAffected;
        } else if (status === 'FAILED' || status === 'ABORTED') {
          const errorMessage = response.Error || 'Unknown error';
          this.logger.error('❌ CleverTap INSERT statement failed', new Error(errorMessage), {
            statementId,
            status,
            error: errorMessage,
          });
          throw new Error(`CleverTap INSERT failed: ${errorMessage}`);
        }
        
        // Wait before next check (2 seconds)
        await new Promise(resolve => setTimeout(resolve, 2000));
      } catch (error) {
        this.logger.error('❌ Error checking CleverTap statement status', error, {
          statementId,
          attempt: i + 1,
        });
        
        // Continue checking even on error (might be transient)
        if (i < maxRetries - 1) {
          await new Promise(resolve => setTimeout(resolve, 2000));
        } else {
          throw error;
        }
      }
    }
    
    this.logger.error('❌ Timeout waiting for CleverTap INSERT to complete', undefined, {
      statementId,
      maxRetries,
      timeoutSeconds: Math.floor((Date.now() - startTime) / 1000),
    });
    throw new Error('Timeout waiting for CleverTap INSERT to complete');
  }

  /**
   * Format value for SQL
   */
  private formatValue(value: any): string {
    if (value === null || value === undefined || value === '') {
      return 'NULL';
    }
    // String value - escape single quotes
    return `'${String(value).replace(/'/g, "''")}'`;
  }

  /**
   * Format date for SQL (DATE type)
   */
  private formatDate(value: any): string {
    if (!value) return 'NULL';
    try {
      // Ensure format is YYYY-MM-DD
      const dateStr = String(value).substring(0, 10);
      if (/^\d{4}-\d{2}-\d{2}$/.test(dateStr)) {
        return `'${dateStr}'`;
      }
      return 'NULL';
    } catch {
      return 'NULL';
    }
  }

  /**
   * Format timestamp for SQL (TIMESTAMP type)
   */
  private formatTimestamp(value: any): string {
    if (!value) return 'NULL';
    try {
      // Ensure format is YYYY-MM-DD HH:MM:SS
      const tsStr = String(value).substring(0, 19);
      if (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(tsStr)) {
        return `'${tsStr}'`;
      }
      return 'NULL';
    } catch {
      return 'NULL';
    }
  }

  /**
   * Format SUPER (JSON) type value for Redshift
   */
  private formatSuperValue(value: any): string {
    if (value === null || value === undefined || value === '') {
      return 'NULL';
    }
    if (typeof value === 'object') {
      return `JSON_PARSE('${JSON.stringify(value).replace(/'/g, "''")}')`;
    }
    if (typeof value === 'string') {
      // If it's already a JSON string, parse it
      try {
        JSON.parse(value);
        return `JSON_PARSE('${value.replace(/'/g, "''")}')`;
      } catch {
        // Not valid JSON, treat as a simple string value
        return `JSON_PARSE('"${value.replace(/"/g, '\\"').replace(/'/g, "''")}"')`;
      }
    }
    // For primitives, wrap in JSON_PARSE
    return `JSON_PARSE('${JSON.stringify(value).replace(/'/g, "''")}')`;
  }
}

