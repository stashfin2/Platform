import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { ExecuteStatementCommand, DescribeStatementCommand } from '@aws-sdk/client-redshift-data';
import { S3Service } from '../services/s3.service';
import { RedshiftClientFactory } from '../config/redshift.config';
import { S3ClientFactory } from '../config/s3.config';
import { LoggerService } from '../services/logger.service';

/**
 * S3 to Redshift Sync Worker
 * Runs every hour to sync data from S3 to Redshift using COPY command
 */
@Service()
export class S3RedshiftSync {
  private isRunning: boolean = false;
  private syncInterval: NodeJS.Timeout | null = null;
  private readonly syncIntervalMs: number;

  constructor(
    @Inject() private readonly s3Service: S3Service,
    @Inject() private readonly redshiftClientFactory: RedshiftClientFactory,
    @Inject() private readonly s3ClientFactory: S3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {
    // Sync interval in milliseconds (default: 1 hour)
    this.syncIntervalMs = parseInt(process.env.S3_SYNC_INTERVAL_MS || '3600000', 10); // 1 hour
  }

  /**
   * Start the sync worker
   */
  async start(): Promise<void> {
    this.isRunning = true;
    this.logger.info('🚀 S3 to Redshift sync worker started', {
      syncIntervalMs: this.syncIntervalMs,
      syncIntervalMinutes: this.syncIntervalMs / 60000,
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
    this.logger.info('⛔ S3 to Redshift sync worker stopped');
  }

  /**
   * Run a single sync operation
   */
  private async runSync(): Promise<void> {
    if (!this.isRunning) {
      return;
    }

    const syncStartTime = Date.now();
    this.logger.info('🔄 Starting S3 to Redshift sync...');

    try {
      // 1. List all pending files in S3
      const pendingFiles = await this.s3Service.listPendingFiles();

      if (pendingFiles.length === 0) {
        this.logger.info('✅ No files to sync');
        return;
      }

      // 2. Use Redshift COPY command to load data from S3
      const statementId = await this.copyFromS3ToRedshift(pendingFiles);

      // 3. Wait for COPY command to complete
      await this.waitForStatementCompletion(statementId);

      // 4. Archive/delete processed files
      await this.s3Service.archiveFiles(pendingFiles);

      const syncDuration = Date.now() - syncStartTime;
      this.logger.info('✅ S3 to Redshift sync completed successfully', {
        fileCount: pendingFiles.length,
        durationMs: syncDuration,
        durationSeconds: Math.floor(syncDuration / 1000),
      });
    } catch (error) {
      this.logger.error('❌ S3 to Redshift sync failed', error);
      // Don't delete files on error - they'll be retried in next sync
    }
  }

  /**
   * Execute Redshift COPY command to load data from S3
   */
  private async copyFromS3ToRedshift(files: string[]): Promise<string> {
    const client = this.redshiftClientFactory.getClient();
    const redshiftConfig = this.redshiftClientFactory.getConfig();
    const s3Config = this.s3ClientFactory.getConfig();

    // Build COPY command
    // Using JSON format with auto option for nested JSON fields
    const sql = `
      COPY ${redshiftConfig.tableName}
      FROM 's3://${s3Config.bucket}/${s3Config.prefix}/'
      IAM_ROLE '${process.env.REDSHIFT_IAM_ROLE || ''}'
      JSON 'auto'
      GZIP
      TIMEFORMAT 'auto'
      DATEFORMAT 'auto'
      TRUNCATECOLUMNS
      BLANKSASNULL
      EMPTYASNULL
      MAXERROR 10;
    `;

    // Alternative: Use access keys instead of IAM role
    // If REDSHIFT_IAM_ROLE is not set, use access keys
    const sqlWithKeys = `
      COPY ${redshiftConfig.tableName}
      FROM 's3://${s3Config.bucket}/${s3Config.prefix}/'
      ACCESS_KEY_ID '${s3Config.accessKeyId}'
      SECRET_ACCESS_KEY '${s3Config.secretAccessKey}'
      JSON 'auto'
      TIMEFORMAT 'auto'
      DATEFORMAT 'auto'
      TRUNCATECOLUMNS
      BLANKSASNULL
      EMPTYASNULL
      MAXERROR 10;
    `;

    const finalSql = process.env.REDSHIFT_IAM_ROLE ? sql : sqlWithKeys;

    this.logger.info('📥 Executing Redshift COPY command', {
      tableName: redshiftConfig.tableName,
      s3Path: `s3://${s3Config.bucket}/${s3Config.prefix}/`,
      fileCount: files.length,
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

      this.logger.info('📨 Redshift COPY command submitted', {
        statementId,
        tableName: redshiftConfig.tableName,
      });

      return statementId;
    } catch (error) {
      this.logger.error('❌ Error executing Redshift COPY command', error);
      throw error;
    }
  }

  /**
   * Wait for Redshift statement to complete
   */
  private async waitForStatementCompletion(statementId: string): Promise<void> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();
    const startTime = Date.now();
    const maxRetries = 180; // 6 minutes (180 * 2 seconds)

    for (let i = 0; i < maxRetries; i++) {
      try {
        const command = new DescribeStatementCommand({ Id: statementId });
        const response = await client.send(command);
        
        const status = response.Status;
        
        if (i > 0 && i % 10 === 0) {
          // Log progress every 10 attempts (every 20 seconds)
          this.logger.info(`⏳ Waiting for Redshift COPY to complete...`, {
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
          this.logger.info('✅ Redshift COPY completed successfully', {
            statementId,
            durationMs: duration,
            durationSeconds: Math.floor(duration / 1000),
            attempts: i + 1,
            rowsAffected,
          });
          return;
        } else if (status === 'FAILED' || status === 'ABORTED') {
          const errorMessage = response.Error || 'Unknown error';
          this.logger.error('❌ Redshift COPY statement failed', new Error(errorMessage), {
            statementId,
            status,
            error: errorMessage,
          });
          throw new Error(`Redshift COPY failed: ${errorMessage}`);
        }
        
        // Wait before next check (2 seconds)
        await new Promise(resolve => setTimeout(resolve, 2000));
      } catch (error) {
        this.logger.error('❌ Error checking statement status', error, {
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
    
    this.logger.error('❌ Timeout waiting for Redshift COPY to complete', undefined, {
      statementId,
      maxRetries,
      timeoutSeconds: Math.floor((Date.now() - startTime) / 1000),
    });
    throw new Error('Timeout waiting for Redshift COPY to complete');
  }

  /**
   * Manual trigger for sync (for testing or manual operations)
   */
  async triggerSync(): Promise<void> {
    this.logger.info('🔧 Manually triggered S3 to Redshift sync');
    await this.runSync();
  }
}

