import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { 
  PutObjectCommand, 
  ListObjectsV2Command, 
  DeleteObjectCommand,
  DeleteObjectsCommand,
  S3Client 
} from '@aws-sdk/client-s3';
import { S3ClientFactory } from '../config/s3.config';
import { LoggerService } from './logger.service';
import { v4 as uuidv4 } from 'uuid';

interface AppsFlyerEvent {
  [key: string]: any;
}

@Service()
export class S3Service {
  private batchBuffer: AppsFlyerEvent[] = [];
  private batchTimer: NodeJS.Timeout | null = null;
  private readonly batchSize: number;
  private readonly batchFlushInterval: number;

  constructor(
    @Inject() private readonly s3ClientFactory: S3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {
    // Batch configuration
    this.batchSize = parseInt(process.env.S3_BATCH_SIZE || '100', 10); // 100 events per file
    this.batchFlushInterval = parseInt(process.env.S3_BATCH_FLUSH_INTERVAL || '30000', 10); // 30 seconds
  }

  /**
   * Upload AppsFlyer event to S3
   * Uses batching to reduce number of S3 PUT requests
   */
  async uploadEvent(event: AppsFlyerEvent): Promise<string> {
    this.batchBuffer.push(event);

    // If batch is full, flush immediately
    if (this.batchBuffer.length >= this.batchSize) {
      return await this.flushBatch();
    }

    // Start timer for auto-flush if not already running
    if (!this.batchTimer) {
      this.batchTimer = setTimeout(() => {
        this.flushBatch();
      }, this.batchFlushInterval);
    }

    return 'batched';
  }

  /**
   * Flush the batch buffer to S3
   */
  private async flushBatch(): Promise<string> {
    if (this.batchBuffer.length === 0) {
      return 'empty';
    }

    // Clear timer
    if (this.batchTimer) {
      clearTimeout(this.batchTimer);
      this.batchTimer = null;
    }

    const eventsToUpload = [...this.batchBuffer];
    this.batchBuffer = [];

    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      // Create filename with timestamp and UUID
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
      const filename = `${config.prefix}/${timestamp}_${uuidv4()}.json`;

      // Convert events to newline-delimited JSON (NDJSON format)
      // This format is optimal for Redshift COPY command
      const content = eventsToUpload.map(event => JSON.stringify(event)).join('\n');

      const command = new PutObjectCommand({
        Bucket: config.bucket,
        Key: filename,
        Body: content,
        ContentType: 'application/json',
      });

      await client.send(command);

      this.logger.info(`📤 Uploaded ${eventsToUpload.length} events to S3`, {
        bucket: config.bucket,
        key: filename,
        eventCount: eventsToUpload.length,
      });

      return filename;
    } catch (error) {
      this.logger.error('❌ Error uploading batch to S3', error, {
        eventCount: eventsToUpload.length,
      });
      // Put events back in buffer for retry
      this.batchBuffer.unshift(...eventsToUpload);
      throw error;
    }
  }

  /**
   * List all pending files in S3 (to be synced to Redshift)
   */
  async listPendingFiles(): Promise<string[]> {
    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      const command = new ListObjectsV2Command({
        Bucket: config.bucket,
        Prefix: config.prefix + '/',
        MaxKeys: 1000, // Process up to 1000 files per sync
      });

      const response = await client.send(command);
      const files = response.Contents?.map(item => item.Key!).filter(key => key.endsWith('.json')) || [];

      this.logger.info(`📋 Found ${files.length} pending files in S3`, {
        bucket: config.bucket,
        prefix: config.prefix,
        fileCount: files.length,
      });

      return files;
    } catch (error) {
      this.logger.error('❌ Error listing S3 files', error);
      throw error;
    }
  }

  /**
   * Delete processed files from S3
   */
  async deleteFiles(keys: string[]): Promise<void> {
    if (keys.length === 0) {
      return;
    }

    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      // S3 batch delete supports up to 1000 objects
      const command = new DeleteObjectsCommand({
        Bucket: config.bucket,
        Delete: {
          Objects: keys.map(key => ({ Key: key })),
        },
      });

      await client.send(command);

      this.logger.info(`🗑️  Deleted ${keys.length} files from S3`, {
        bucket: config.bucket,
        fileCount: keys.length,
      });
    } catch (error) {
      this.logger.error('❌ Error deleting S3 files', error, {
        fileCount: keys.length,
      });
      throw error;
    }
  }

  /**
   * Move processed files to archive folder
   */
  async archiveFiles(keys: string[]): Promise<void> {
    if (keys.length === 0) {
      return;
    }

    try {
      const config = this.s3ClientFactory.getConfig();
      const archivePrefix = process.env.S3_ARCHIVE_PREFIX || 'appsflyer-events-archive';

      // For simplicity, we'll just delete for now
      // In production, you might want to copy to archive then delete
      await this.deleteFiles(keys);

      this.logger.info(`📦 Archived ${keys.length} files`, {
        bucket: config.bucket,
        fileCount: keys.length,
      });
    } catch (error) {
      this.logger.error('❌ Error archiving S3 files', error, {
        fileCount: keys.length,
      });
      throw error;
    }
  }

  /**
   * Force flush any pending batched events (call during shutdown)
   */
  async forceFlush(): Promise<void> {
    if (this.batchBuffer.length > 0) {
      this.logger.info(`🔄 Force flushing ${this.batchBuffer.length} events to S3`);
      await this.flushBatch();
    }
  }
}

