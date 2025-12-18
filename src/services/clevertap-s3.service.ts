import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { 
  ListObjectsV2Command, 
  DeleteObjectsCommand,
} from '@aws-sdk/client-s3';
import { CleverTapS3ClientFactory } from '../config/clevertap.config';
import { LoggerService } from './logger.service';

@Service()
export class CleverTapS3Service {
  private readonly startDate: Date;
  private readonly startTimestamp: number;

  constructor(
    @Inject() private readonly s3ClientFactory: CleverTapS3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {
    // Filter files from October 2024 onwards (default: October 1, 2024)
    const startDateStr = process.env.CLEVERTAP_START_DATE || '2024-10-01';
    this.startDate = new Date(startDateStr);
    this.startTimestamp = Math.floor(this.startDate.getTime() / 1000);
    
    this.logger.info('📅 CleverTap date filter configured', {
      startDate: startDateStr,
      startTimestamp: this.startTimestamp,
      filterDescription: `Only syncing files from ${startDateStr} onwards`,
    });
  }

  /**
   * List all pending CSV files in CleverTap S3 bucket
   * Filters files to only include those from October 2024 onwards based on filename timestamp
   */
  async listPendingFiles(): Promise<string[]> {
    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      const command = new ListObjectsV2Command({
        Bucket: config.bucket,
        Prefix: config.prefix,
        MaxKeys: 1000, // Process up to 1000 files per sync
      });

      const response = await client.send(command);
      
      // Filter for CSV and compressed CSV files
      let allFiles = response.Contents?.map(item => item.Key!)
        .filter(key => key.endsWith('.csv') || key.endsWith('.csv.gz')) || [];

      this.logger.info(`📋 Found ${allFiles.length} total CleverTap CSV files in S3`, {
        bucket: config.bucket,
        prefix: config.prefix,
      });

      // Filter files by date (only from October 2024 onwards)
      const files = allFiles.filter(filename => {
        try {
          // Extract timestamp from filename
          // Format: timestamp1-timestamp2-timestamp3-...csv.gz
          // The SECOND timestamp is usually the data/event date
          const parts = filename.split('/').pop()?.split('-') || [];
          
          if (parts.length >= 2) {
            // Try the second timestamp first (data date for most files)
            const fileTimestamp = parseInt(parts[1], 10);
            
            if (!isNaN(fileTimestamp) && fileTimestamp >= this.startTimestamp) {
              return true;
            }
          }
          
          // Fallback to first timestamp if second doesn't exist or is invalid
          if (parts.length > 0) {
            const fallbackTimestamp = parseInt(parts[0], 10);
            if (!isNaN(fallbackTimestamp) && fallbackTimestamp >= this.startTimestamp) {
              return true;
            }
          }
          
          return false;
        } catch (error) {
          this.logger.warn('⚠️  Could not parse timestamp from filename, including file', {
            filename,
          });
          return true; // Include file if we can't parse the date
        }
      });

      const filteredCount = allFiles.length - files.length;
      
      this.logger.info(`📋 Filtered CleverTap files by date`, {
        totalFiles: allFiles.length,
        filesAfterFilter: files.length,
        filteredOut: filteredCount,
        startDate: this.startDate.toISOString().split('T')[0],
        sampleFile: files[0] || 'none',
      });

      return files;
    } catch (error) {
      this.logger.error('❌ Error listing CleverTap S3 files', error);
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

      this.logger.info(`🗑️  Deleted ${keys.length} CleverTap files from S3`, {
        bucket: config.bucket,
        fileCount: keys.length,
      });
    } catch (error) {
      this.logger.error('❌ Error deleting CleverTap S3 files', error, {
        fileCount: keys.length,
      });
      throw error;
    }
  }

  /**
   * Archive processed files (currently just deletes them)
   */
  async archiveFiles(keys: string[]): Promise<void> {
    if (keys.length === 0) {
      return;
    }

    try {
      const config = this.s3ClientFactory.getConfig();

      // For simplicity, we'll just delete for now
      // In production, you might want to copy to archive then delete
      await this.deleteFiles(keys);

      this.logger.info(`📦 Archived ${keys.length} CleverTap files`, {
        bucket: config.bucket,
        fileCount: keys.length,
      });
    } catch (error) {
      this.logger.error('❌ Error archiving CleverTap S3 files', error, {
        fileCount: keys.length,
      });
      throw error;
    }
  }
}

