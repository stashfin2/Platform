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
    // Sync interval in milliseconds (default: 30 minutes)
    this.syncIntervalMs = parseInt(process.env.CLEVERTAP_SYNC_INTERVAL_MS || '1800000', 10);
    // CleverTap table name - using new campaign events table
    this.tableName = process.env.CLEVERTAP_TABLE_NAME || 'appsflyer.clever_tap_campaign_events';
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
    const syncTimestamp = new Date().toISOString();
    
    this.logger.info('═══════════════════════════════════════════════════════════════');
    this.logger.info('🔄 Starting CleverTap S3 to Redshift sync', {
      timestamp: syncTimestamp,
      table: this.tableName,
      bucket: this.clevertapS3ClientFactory.getConfig().bucket,
    });
    this.logger.info('═══════════════════════════════════════════════════════════════');

    let jsonPath = '';
    let totalRowsProcessed = 0;

    try {
      // 1. List all pending CSV files in S3 (from October onwards)
      this.logger.info('📂 Step 1/6: Listing pending CSV files from S3...');
      const pendingFiles = await this.clevertapS3Service.listPendingFiles();

      if (pendingFiles.length === 0) {
        this.logger.info('✅ No CleverTap files to sync (all caught up!)');
        this.logger.info('Next sync in ' + Math.floor(this.syncIntervalMs / 60000) + ' minutes');
        return;
      }

      this.logger.info('📋 Found files to process:', {
        totalFiles: pendingFiles.length,
        firstFile: pendingFiles[0],
        lastFile: pendingFiles[pendingFiles.length - 1],
      });

      // 2. Convert CSV files to JSON format (for schema evolution support)
      this.logger.info('📝 Step 2/6: Converting CSV files to JSON format...');
      const conversionStartTime = Date.now();
      jsonPath = await this.csvToJsonConverter.convertCsvFilesToJson(pendingFiles);
      const conversionDuration = Date.now() - conversionStartTime;
      
      this.logger.info('✅ CSV to JSON conversion completed', {
        durationSeconds: Math.floor(conversionDuration / 1000),
        filesConverted: pendingFiles.length,
      });

      // 3. Use Redshift COPY command to load JSON data from S3 (with schema evolution)
      this.logger.info('📥 Step 3/6: Executing Redshift COPY command...');
      const copyStartTime = Date.now();
      const statementId = await this.copyFromS3ToRedshift();
      this.logger.info('✅ COPY command submitted', { statementId });

      // 4. Wait for COPY command to complete
      this.logger.info('⏳ Step 4/6: Waiting for COPY to complete...');
      const rowsLoaded = await this.waitForStatementCompletion(statementId);
      const copyDuration = Date.now() - copyStartTime;
      totalRowsProcessed = rowsLoaded;
      
      this.logger.info('✅ Data loaded to Redshift successfully', {
        rowsLoaded: totalRowsProcessed,
        durationSeconds: Math.floor(copyDuration / 1000),
        table: this.tableName,
      });

      // 5. Clean up converted JSON files
      this.logger.info('🗑️  Step 5/6: Cleaning up temporary JSON files...');
      await this.csvToJsonConverter.cleanupJsonFiles();
      this.logger.info('✅ JSON files cleaned up');

      // 6. Archive/delete original CSV files
      this.logger.info('📦 Step 6/6: Archiving processed CSV files...');
      await this.clevertapS3Service.archiveFiles(pendingFiles);
      this.logger.info('✅ CSV files archived');

      const syncDuration = Date.now() - syncStartTime;
      
      this.logger.info('═══════════════════════════════════════════════════════════════');
      this.logger.info('✅ SYNC COMPLETED SUCCESSFULLY', {
        timestamp: new Date().toISOString(),
        filesProcessed: pendingFiles.length,
        rowsLoaded: totalRowsProcessed,
        totalDurationSeconds: Math.floor(syncDuration / 1000),
        totalDurationMinutes: Math.floor(syncDuration / 60000),
        table: this.tableName,
        nextSyncIn: Math.floor(this.syncIntervalMs / 60000) + ' minutes',
      });
      this.logger.info('═══════════════════════════════════════════════════════════════');
    } catch (error) {
      this.logger.error('═══════════════════════════════════════════════════════════════');
      this.logger.error('❌ SYNC FAILED', {
        timestamp: new Date().toISOString(),
        errorMessage: error instanceof Error ? error.message : String(error),
        table: this.tableName,
      });
      this.logger.error('❌ CleverTap S3 to Redshift sync failed', error);
      this.logger.error('═══════════════════════════════════════════════════════════════');
      
      // Clean up JSON files if conversion succeeded but COPY failed
      if (jsonPath) {
        try {
          this.logger.info('🗑️  Cleaning up JSON files after error...');
          await this.csvToJsonConverter.cleanupJsonFiles();
        } catch (cleanupError) {
          this.logger.error('Error cleaning up JSON files after failure', cleanupError);
        }
      }
      
      // Don't delete CSV files on error - they'll be retried in next sync
      this.logger.info('⏭️  CSV files preserved for retry in next sync cycle');
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
   * Returns the number of rows loaded
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
          return rowsAffected;
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

