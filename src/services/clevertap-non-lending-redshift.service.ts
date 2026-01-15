import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { ExecuteStatementCommand, DescribeStatementCommand } from '@aws-sdk/client-redshift-data';
import { RedshiftClientFactory } from '../config/redshift.config';
import { LoggerService } from './logger.service';

@Service()
export class CleverTapNonLendingRedshiftService {
  private readonly schemaName = 'appsflyer';
  private readonly tableName = 'clevertap_non_lending_data';

  constructor(
    @Inject() private readonly redshiftClientFactory: RedshiftClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * Create the Redshift table if it doesn't exist
   */
  async createTableIfNotExists(): Promise<void> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();

    // Create staging table for raw JSON
    const createStagingTableSql = `
CREATE TABLE IF NOT EXISTS ${this.schemaName}.${this.tableName}_staging
(
	raw_data SUPER   ENCODE zstd
)
DISTSTYLE AUTO;
    `.trim();

    // Create final table
    const createTableSql = `
CREATE TABLE IF NOT EXISTS ${this.schemaName}.${this.tableName}
(
	event_date DATE   ENCODE RAW
	,event_ts TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,user_id VARCHAR(64)   ENCODE lzo
	,event_name VARCHAR(128)   ENCODE RAW
	,source_file VARCHAR(512)   ENCODE lzo
	,attributes SUPER   ENCODE zstd
)
DISTSTYLE AUTO
 SORTKEY (
	event_date
	, event_name
	)
;
    `.trim();

    const alterTableSql = `
ALTER TABLE ${this.schemaName}.${this.tableName} owner to stashuser;
    `.trim();

    try {
      this.logger.info('📋 Creating Redshift tables if not exists', {
        schema: this.schemaName,
        table: this.tableName,
        stagingTable: `${this.tableName}_staging`,
      });

      // Create staging table
      const createStagingCommand = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: createStagingTableSql,
      });

      const createStagingResponse = await client.send(createStagingCommand);
      const createStagingStatementId = createStagingResponse.Id || '';
      await this.waitForStatementCompletion(createStagingStatementId, 'CREATE STAGING TABLE');

      // Create final table
      const createCommand = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: createTableSql,
      });

      const createResponse = await client.send(createCommand);
      const createStatementId = createResponse.Id || '';

      await this.waitForStatementCompletion(createStatementId, 'CREATE TABLE');

      // Alter table owner
      const alterCommand = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: alterTableSql,
      });

      const alterResponse = await client.send(alterCommand);
      const alterStatementId = alterResponse.Id || '';

      await this.waitForStatementCompletion(alterStatementId, 'ALTER TABLE');

      this.logger.info('✅ Redshift table created/verified successfully', {
        schema: this.schemaName,
        table: this.tableName,
      });
    } catch (error) {
      this.logger.error('❌ Error creating Redshift table', error, {
        schema: this.schemaName,
        table: this.tableName,
      });
      throw error;
    }
  }

  /**
   * Wait for Redshift statement to complete
   */
  private async waitForStatementCompletion(
    statementId: string,
    operation: string
  ): Promise<void> {
    const client = this.redshiftClientFactory.getClient();
    const startTime = Date.now();
    const maxRetries = 30; // 1 minute (30 * 2 seconds)

    for (let i = 0; i < maxRetries; i++) {
      try {
        const command = new DescribeStatementCommand({ Id: statementId });
        const response = await client.send(command);
        
        const status = response.Status;
        
        if (status === 'FINISHED') {
          const duration = Date.now() - startTime;
          this.logger.info(`✅ ${operation} completed successfully`, {
            statementId,
            durationMs: duration,
            durationSeconds: Math.floor(duration / 1000),
          });
          return;
        } else if (status === 'FAILED' || status === 'ABORTED') {
          const errorMessage = response.Error || 'Unknown error';
          this.logger.error(`❌ ${operation} failed`, new Error(errorMessage), {
            statementId,
            status,
            error: errorMessage,
          });
          throw new Error(`${operation} failed: ${errorMessage}`);
        }
        
        // Wait before next check (2 seconds)
        await new Promise(resolve => setTimeout(resolve, 2000));
      } catch (error) {
        this.logger.error(`❌ Error checking ${operation} status`, error, {
          statementId,
          attempt: i + 1,
        });
        
        if (i < maxRetries - 1) {
          await new Promise(resolve => setTimeout(resolve, 2000));
        } else {
          throw error;
        }
      }
    }
    
    throw new Error(`Timeout waiting for ${operation} to complete`);
  }

  /**
   * Get the full table name (schema.table)
   */
  getFullTableName(): string {
    return `${this.schemaName}.${this.tableName}`;
  }

  /**
   * Get the full staging table name (schema.table_staging)
   */
  getFullStagingTableName(): string {
    return `${this.schemaName}.${this.tableName}_staging`;
  }

  /**
   * Truncate the table to remove all data
   */
  async truncateTable(): Promise<void> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();
    const tableName = this.getFullTableName();

    const truncateSql = `TRUNCATE TABLE ${tableName};`;

    try {
      this.logger.info('🗑️  Truncating table', { tableName });

      const command = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: truncateSql,
      });

      const response = await client.send(command);
      const statementId = response.Id || '';

      await this.waitForStatementCompletion(statementId, 'TRUNCATE TABLE');

      this.logger.info('✅ Table truncated successfully', { tableName });
    } catch (error) {
      this.logger.error('❌ Error truncating table', error, { tableName });
      throw error;
    }
  }
}

