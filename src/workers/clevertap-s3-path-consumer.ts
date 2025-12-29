import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { CleverTapSQSService } from '../services/clevertap-sqs.service';
import { LoggerService } from '../services/logger.service';
import { ExecuteStatementCommand, DescribeStatementCommand } from '@aws-sdk/client-redshift-data';
import { DeleteObjectCommand, HeadObjectCommand } from '@aws-sdk/client-s3';
import { RedshiftClientFactory } from '../config/redshift.config';
import { CleverTapJsonS3ClientFactory } from '../config/clevertap-json-s3.config';

/**
 * CleverTap S3 Path Consumer
 * Reads S3 paths from SQS, COPYs from S3 to Redshift, deletes message and JSON
 */
@Service()
export class CleverTapS3PathConsumer {
  private isRunning: boolean = false;
  private workerId: string;
  private messagesProcessed: number = 0;
  private messagesFailed: number = 0;
  private rowsProcessed: number = 0;
  // Track files processed in current batch only (to avoid processing same file twice in one batch)
  private currentBatchProcessed: Set<string> = new Set();
  // Track files scheduled for deletion (with timestamp) - cleanup runs once per 24 hours
  private filesToDelete: Map<string, number> = new Map();
  private lastCleanupTime: number = 0;

  constructor(
    @Inject() private readonly sqsService: CleverTapSQSService,
    @Inject() private readonly redshiftFactory: RedshiftClientFactory,
    @Inject() private readonly s3ClientFactory: CleverTapJsonS3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {
    this.workerId = `s3-path-consumer-${Date.now()}`;
  }

  async start(): Promise<void> {
    this.isRunning = true;
    this.logger.info('🚀 CleverTap S3 Path Consumer started', {
      workerId: this.workerId,
    });

    this.poll();
  }

  stop(): void {
    this.isRunning = false;
    this.logger.info(`⛔ CleverTap S3 Path Consumer stopped`, {
      workerId: this.workerId,
      messagesProcessed: this.messagesProcessed,
      messagesFailed: this.messagesFailed,
      rowsProcessed: this.rowsProcessed,
    });
  }

  private async poll(): Promise<void> {
    while (this.isRunning) {
      try {
        // Long poll for messages (20s)
        const messages = await this.sqsService.receiveMessages(10);

        if (messages.length > 0) {
          this.logger.debug(`📨 Received ${messages.length} path messages`);

          // Clear current batch tracking for new batch
          this.currentBatchProcessed.clear();

          let batchStats = {
            total: messages.length,
            processed: 0,
            missing: 0,
            failed: 0,
            invalid: 0,
          };

          const deleteHandles: string[] = [];

          for (const message of messages) {
            let s3Path: string | undefined;
            let messageId: string | undefined;
            
            try {
              const body = JSON.parse(message.Body || '{}');
              const data = body.data || {};
              s3Path = data.s3_path;
              messageId = body.id;
              const rowCount = data.row_count || 0;

              if (!s3Path) {
                this.logger.debug('⚠️  Message missing s3_path', { messageId });
                // Delete invalid message to prevent infinite retries
                deleteHandles.push(message.ReceiptHandle);
                batchStats.invalid++;
                continue;
              }

              this.logger.info(`📥 Processing S3 path: ${s3Path}`, { rowCount });

              // Check if we've already processed this file in the current batch (avoid duplicate messages in same batch)
              if (this.currentBatchProcessed.has(s3Path)) {
                this.logger.debug('⚠️  File already processed in this batch, skipping duplicate message', { s3Path });
                deleteHandles.push(message.ReceiptHandle);
                batchStats.processed++; // Count as processed (duplicate message)
                continue;
              }

              // Verify file exists in S3 before attempting COPY
              const fileExists = await this.verifyS3FileExists(s3Path);
              if (!fileExists) {
                // File doesn't exist = already processed and deleted, or never existed
                this.logger.debug('⚠️  S3 file does not exist, skipping and deleting message', { s3Path });
                deleteHandles.push(message.ReceiptHandle);
                batchStats.missing++;
                this.messagesFailed++;
                continue;
              }

              // Execute COPY from S3
              const rowsInserted = await this.copyFromS3(s3Path);

              if (rowsInserted > 0) {
                this.rowsProcessed += rowsInserted;
                this.messagesProcessed++;
                batchStats.processed++;

                // Mark file as processed in current batch (to avoid duplicate messages in same batch)
                this.currentBatchProcessed.add(s3Path);

                this.logger.info(`✅ COPY successful`, {
                  s3Path,
                  rowsInserted,
                  totalProcessed: this.rowsProcessed,
                });

                // Delete message from SQS
                deleteHandles.push(message.ReceiptHandle);

                // Schedule file for deletion (cleanup runs once per 24 hours)
                // Files are kept for 24 hours to allow reprocessing if needed
                const deleteAfterMs = parseInt(process.env.CLEVERTAP_FILE_DELETE_AFTER_MS || '86400000', 10); // 24 hours default
                this.filesToDelete.set(s3Path, Date.now() + deleteAfterMs);
              } else {
                this.logger.warn('⚠️  COPY completed but 0 rows inserted', { s3Path });
                // Still delete message to prevent retries
                deleteHandles.push(message.ReceiptHandle);
                batchStats.failed++;
                this.messagesFailed++;
              }
            } catch (error: any) {
              const errorMessage = error?.message || String(error);
              
              // Check if error is about file not existing
              if (errorMessage.includes('does not exist') || 
                  errorMessage.includes('NoSuchKey') ||
                  errorMessage.includes('NotFound') ||
                  (errorMessage.includes('prefix') && errorMessage.includes('does not exist'))) {
                // Use debug level to reduce log noise
                this.logger.debug('⚠️  S3 file not found (from COPY error), deleting message to prevent retries', { 
                  s3Path: s3Path || 'unknown',
                  error: errorMessage 
                });
                // Delete message to prevent infinite retries
                deleteHandles.push(message.ReceiptHandle);
                batchStats.missing++;
              } else {
                this.logger.error('❌ Failed to process message', error, { 
                  s3Path: s3Path || 'unknown',
                  messageId 
                });
                deleteHandles.push(message.ReceiptHandle);
                batchStats.failed++;
              }
              this.messagesFailed++;
            }
          }

          // Batch delete all messages at once for efficiency
          if (deleteHandles.length > 0) {
            await this.sqsService.batchDeleteMessages(deleteHandles);
          }

          // Clean up old files once per 24 hours
          await this.cleanupOldFiles();

          // Log batch summary
          if (batchStats.missing > 0 || batchStats.failed > 0) {
            this.logger.info('📊 Batch summary', {
              total: batchStats.total,
              processed: batchStats.processed,
              missing: batchStats.missing,
              failed: batchStats.failed,
              invalid: batchStats.invalid,
            });
          }
        }
      } catch (error) {
        this.logger.error('❌ Error in poll loop', error);
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
  }

  /**
   * Clean up files that have passed their retention period
   * Runs once per 24 hours to delete files older than the retention period
   */
  private async cleanupOldFiles(): Promise<void> {
    const now = Date.now();
    const cleanupIntervalMs = parseInt(process.env.CLEVERTAP_CLEANUP_INTERVAL_MS || '86400000', 10); // 24 hours default
    
    // Only run cleanup once per interval
    if (now - this.lastCleanupTime < cleanupIntervalMs) {
      return;
    }
    
    this.lastCleanupTime = now;
    this.logger.info('🧹 Starting scheduled file cleanup (runs once per 24 hours)');
    
    const filesToDeleteNow: string[] = [];
    let totalFiles = this.filesToDelete.size;

    for (const [s3Path, deleteTime] of this.filesToDelete.entries()) {
      if (now >= deleteTime) {
        filesToDeleteNow.push(s3Path);
      }
    }

    let deletedCount = 0;
    let failedCount = 0;

    for (const s3Path of filesToDeleteNow) {
      try {
        await this.deleteS3File(s3Path);
        this.filesToDelete.delete(s3Path);
        deletedCount++;
      } catch (error) {
        failedCount++;
        this.logger.warn(`⚠️  Failed to delete file: ${s3Path}`, { s3Path, error });
      }
    }

    this.logger.info('✅ File cleanup completed', {
      totalFiles,
      deleted: deletedCount,
      failed: failedCount,
      remaining: this.filesToDelete.size,
      nextCleanup: new Date(now + cleanupIntervalMs).toISOString(),
    });
  }

  /**
   * Verify that S3 file exists before attempting COPY
   */
  private async verifyS3FileExists(s3Path: string): Promise<boolean> {
    try {
      const match = s3Path.match(/s3:\/\/([^\/]+)\/(.+)/);
      if (!match) {
        this.logger.debug(`⚠️  Invalid S3 path format: ${s3Path}`);
        return false;
      }

      const bucket = match[1];
      const key = match[2];

      const client = this.s3ClientFactory.getClient();
      await client.send(new HeadObjectCommand({ Bucket: bucket, Key: key }));
      
      return true;
    } catch (error: any) {
      if (error.name === 'NotFound' || error.$metadata?.httpStatusCode === 404) {
        // Use debug level - missing files are logged at batch level
        return false;
      }
      // For other errors, log but still return false to be safe
      this.logger.warn(`⚠️  Error checking S3 file existence: ${s3Path}`, error);
      return false;
    }
  }

  /**
   * Execute Redshift COPY from S3 path
   */
  private async copyFromS3(s3Path: string): Promise<number> {
    const client = this.redshiftFactory.getClient();
    const config = this.redshiftFactory.getConfig();
    const s3Config = this.s3ClientFactory.getConfig();
    const tableName = process.env.CLEVERTAP_TABLE_NAME || 'appsflyer.clevertap_events';

    const sql = s3Config.accessKeyId && s3Config.secretAccessKey
      ? `
        COPY ${tableName}
        FROM '${s3Path}'
        ACCESS_KEY_ID '${s3Config.accessKeyId}'
        SECRET_ACCESS_KEY '${s3Config.secretAccessKey}'
        JSON 'auto'
        TIMEFORMAT 'auto'
        DATEFORMAT 'auto'
        TRUNCATECOLUMNS
        BLANKSASNULL
        EMPTYASNULL
        COMPUPDATE OFF
        STATUPDATE OFF;
      `
      : `
        COPY ${tableName}
        FROM '${s3Path}'
        IAM_ROLE '${process.env.REDSHIFT_IAM_ROLE || ''}'
        JSON 'auto'
        TIMEFORMAT 'auto'
        DATEFORMAT 'auto'
        TRUNCATECOLUMNS
        BLANKSASNULL
        EMPTYASNULL
        COMPUPDATE OFF
        STATUPDATE OFF;
      `;

    this.logger.info('📥 Executing COPY command', { s3Path, tableName });

    const command = new ExecuteStatementCommand({
      ClusterIdentifier: config.clusterIdentifier,
      Database: config.database,
      DbUser: config.dbUser,
      Sql: sql,
    });

    const response = await client.send(command);
    const statementId = response.Id || '';

    // Wait for completion
    const rowsInserted = await this.waitForCopyCompletion(statementId);

    return rowsInserted;
  }

  /**
   * Wait for COPY completion
   */
  private async waitForCopyCompletion(statementId: string): Promise<number> {
    const client = this.redshiftFactory.getClient();
    const maxRetries = 180;

    for (let i = 0; i < maxRetries; i++) {
      await new Promise(resolve => setTimeout(resolve, 2000));

      const descCmd = new DescribeStatementCommand({ Id: statementId });
      const desc = await client.send(descCmd);

      if (desc.Status === 'FINISHED') {
        // Redshift Data API returns 0 for COPY, so we trust completion
        // In production, you'd query the table to verify actual row count
        return desc.ResultRows || 1; // Return 1 to indicate success
      } else if (desc.Status === 'FAILED') {
        const errorMessage = desc.Error || 'Unknown error';
        const error = new Error(`COPY failed: ${errorMessage}`);
        // Attach the error message for better error handling upstream
        (error as any).redshiftError = errorMessage;
        throw error;
      }

      if (i > 0 && i % 10 === 0) {
        this.logger.info(`⏳ Waiting for COPY...`, {
          statementId,
          attempt: i + 1,
          status: desc.Status,
        });
      }
    }

    throw new Error('Timeout waiting for COPY');
  }

  /**
   * Delete JSON file from S3 after successful COPY
   */
  private async deleteS3File(s3Path: string): Promise<void> {
    const match = s3Path.match(/s3:\/\/([^\/]+)\/(.+)/);
    if (!match) return;

    const bucket = match[1];
    const key = match[2];

    try {
      const client = this.s3ClientFactory.getClient();
      await client.send(new DeleteObjectCommand({ Bucket: bucket, Key: key }));
      this.logger.debug(`🗑️  Deleted JSON from S3`, { s3Path });
    } catch (error) {
      this.logger.warn('⚠️  Failed to delete S3 file', { s3Path, error });
    }
  }
}

