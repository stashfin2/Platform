import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { ExecuteStatementCommand, DescribeStatementCommand } from '@aws-sdk/client-redshift-data';
import { CleverTapS3Service } from '../services/clevertap-s3.service';
import { CsvToJsonConverterService } from '../services/csv-to-json-converter.service';
import { RedshiftClientFactory } from '../config/redshift.config';
import { CleverTapS3ClientFactory } from '../config/clevertap.config';
import { LoggerService } from '../services/logger.service';

/**
 * CleverTap S3 to Redshift Sync Worker
 * Syncs CleverTap campaign data from S3 to Redshift
 * Converts CSV to JSON for automatic schema evolution (same as AppsFlyer)
 */
@Service()
export class CleverTapSync {
  private isRunning: boolean = false;
  private syncInterval: NodeJS.Timeout | null = null;
  private readonly syncIntervalMs: number;
  private readonly tableName: string;

  constructor(
    @Inject() private readonly clevertapS3Service: CleverTapS3Service,
    @Inject() private readonly csvToJsonConverter: CsvToJsonConverterService,
    @Inject() private readonly redshiftClientFactory: RedshiftClientFactory,
    @Inject() private readonly clevertapS3ClientFactory: CleverTapS3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {
    // Sync interval in milliseconds (default: 1 hour)
    this.syncIntervalMs = parseInt(process.env.CLEVERTAP_SYNC_INTERVAL_MS || '3600000', 10);
    // CleverTap table name
    this.tableName = process.env.CLEVERTAP_TABLE_NAME || 'appsflyer.clevertap_events';
  }

  /**
   * Start the sync worker
   */
  async start(): Promise<void> {
    this.isRunning = true;
    this.logger.info('🚀 CleverTap S3 to Redshift sync worker started', {
      syncIntervalMs: this.syncIntervalMs,
      syncIntervalMinutes: this.syncIntervalMs / 60000,
      tableName: this.tableName,
    });

    // Run initial sync immediately
    await this.runSync();

    // Schedule periodic syncs
    this.syncInterval = setInterval(() => {
      this.runSync();
    }, this.syncIntervalMs);
  }

  /**
   * Stop the sync worker
   */
  stop(): void {
    this.isRunning = false;
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }
    this.logger.info('⛔ CleverTap S3 to Redshift sync worker stopped');
  }

  /**
   * Run a single sync operation
   */
  private async runSync(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    const syncStartTime = Date.now();
    this.logger.info('🔄 Starting CleverTap S3 to Redshift sync...');

    let jsonPath = '';

    try {
      // 1. List all pending CSV files in S3
      const pendingFiles = await this.clevertapS3Service.listPendingFiles();

      if (pendingFiles.length === 0) {
        this.logger.info('✅ No CleverTap files to sync');
        return;
      }

      // 2. Convert CSV files to JSON format (for schema evolution support)
      this.logger.info('📝 Converting CSV files to JSON format...');
      jsonPath = await this.csvToJsonConverter.convertCsvFilesToJson(pendingFiles);

      // 3. Use Redshift COPY command to load JSON data from S3 (with schema evolution)
      const statementId = await this.copyFromS3ToRedshift();

      // 4. Wait for COPY command to complete
      await this.waitForStatementCompletion(statementId);

      // 5. Clean up converted JSON files
      await this.csvToJsonConverter.cleanupJsonFiles();

      // 6. Archive/delete original CSV files
      await this.clevertapS3Service.archiveFiles(pendingFiles);

      const syncDuration = Date.now() - syncStartTime;
      this.logger.info('✅ CleverTap S3 to Redshift sync completed successfully', {
        fileCount: pendingFiles.length,
        durationMs: syncDuration,
        durationSeconds: Math.floor(syncDuration / 1000),
      });
    } catch (error) {
      this.logger.error('❌ CleverTap S3 to Redshift sync failed', error);
      
      // Clean up JSON files if conversion succeeded but COPY failed
      if (jsonPath) {
        try {
          await this.csvToJsonConverter.cleanupJsonFiles();
        } catch (cleanupError) {
          this.logger.error('Error cleaning up JSON files after failure', cleanupError);
        }
      }
      
      // Don't delete CSV files on error - they'll be retried in next sync
    }
  }

  /**
   * Execute Redshift COPY command to load JSON data from S3
   * Uses JSON 'auto' for automatic schema evolution (same as AppsFlyer)
   */
  private async copyFromS3ToRedshift(): Promise<string> {
    const client = this.redshiftClientFactory.getClient();
    const redshiftConfig = this.redshiftClientFactory.getConfig();
    const s3Config = this.clevertapS3ClientFactory.getConfig();

    // Get JSON prefix from environment or use default
    const jsonPrefix = process.env.CLEVERTAP_JSON_PREFIX || 'clevertap-json';

    // Build COPY command using JSON 'auto' - SAME AS APPSFLYER
    // This enables automatic schema evolution!
    const sqlWithKeys = `
      COPY ${this.tableName}
      FROM 's3://${s3Config.bucket}/${jsonPrefix}/'
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
    `;

    // Alternative: Use IAM role if configured
    const sqlWithRole = `
      COPY ${this.tableName}
      FROM 's3://${s3Config.bucket}/${jsonPrefix}/'
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

    const finalSql = process.env.REDSHIFT_IAM_ROLE ? sqlWithRole : sqlWithKeys;

    this.logger.info('📥 Executing Redshift COPY command for CleverTap data (JSON auto mode)', {
      tableName: this.tableName,
      s3Path: `s3://${s3Config.bucket}/${jsonPrefix}/`,
      format: 'JSON auto (schema evolution enabled)',
    });

    try {
      const command = new ExecuteStatementCommand({
        ClusterIdentifier: redshiftConfig.clusterIdentifier,
        Database: redshiftConfig.database,
        DbUser: redshiftConfig.dbUser,
        Sql: finalSql,
      });

      const response = await client.send(command);
      const statementId = response.Id || '';

      this.logger.info('📨 CleverTap COPY command submitted', {
        statementId,
        tableName: this.tableName,
      });

      return statementId;
    } catch (error) {
      this.logger.error('❌ Error executing CleverTap COPY command', error);
      throw error;
    }
  }

  /**
   * Wait for Redshift statement to complete
   */
  private async waitForStatementCompletion(statementId: string): Promise<void> {
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
          this.logger.info(`⏳ Waiting for CleverTap COPY to complete...`, {
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
          this.logger.info('✅ CleverTap COPY completed successfully', {
            statementId,
            durationMs: duration,
            durationSeconds: Math.floor(duration / 1000),
            attempts: i + 1,
            rowsAffected,
          });
          return;
        } else if (status === 'FAILED' || status === 'ABORTED') {
          const errorMessage = response.Error || 'Unknown error';
          this.logger.error('❌ CleverTap COPY statement failed', new Error(errorMessage), {
            statementId,
            status,
            error: errorMessage,
          });
          throw new Error(`CleverTap COPY failed: ${errorMessage}`);
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
    
    this.logger.error('❌ Timeout waiting for CleverTap COPY to complete', undefined, {
      statementId,
      maxRetries,
      timeoutSeconds: Math.floor((Date.now() - startTime) / 1000),
    });
    throw new Error('Timeout waiting for CleverTap COPY to complete');
  }

  /**
   * Manual trigger for sync (for testing or manual operations)
   */
  async triggerSync(): Promise<void> {
    this.logger.info('🔧 Manually triggered CleverTap S3 to Redshift sync');
    await this.runSync();
  }
}

