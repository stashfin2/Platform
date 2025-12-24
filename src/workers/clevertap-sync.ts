import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { CleverTapS3Service } from '../services/clevertap-s3.service';
import { CleverTapCsvToSqsService } from '../services/clevertap-csv-to-sqs.service';
import { LoggerService } from '../services/logger.service';

/**
 * CleverTap S3 to SQS Sync Worker
 * Syncs CleverTap campaign data from S3 to SQS
 * Flow: CSV → JSON → SQS → (SQS Consumer) → Redshift
 * Memory-efficient: No intermediate storage, streams directly to SQS
 */
@Service()
export class CleverTapSync {
  private isRunning: boolean = false;
  private syncInterval: NodeJS.Timeout | null = null;
  private readonly syncIntervalMs: number;

  constructor(
    @Inject() private readonly clevertapS3Service: CleverTapS3Service,
    @Inject() private readonly csvToSqsService: CleverTapCsvToSqsService,
    @Inject() private readonly logger: LoggerService
  ) {
    // Sync interval in milliseconds (default: 30 minutes)
    this.syncIntervalMs = parseInt(process.env.CLEVERTAP_SYNC_INTERVAL_MS || '1800000', 10);
  }

  /**
   * Start the sync worker
   */
  async start(): Promise<void> {
    this.isRunning = true;
    this.logger.info('🚀 CleverTap S3 to SQS sync worker started', {
      syncIntervalMs: this.syncIntervalMs,
      syncIntervalMinutes: this.syncIntervalMs / 60000,
      mode: 'CSV → JSON → SQS → Redshift',
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
    this.logger.info('⛔ CleverTap S3 to SQS sync worker stopped');
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
    this.logger.info('🔄 Starting CleverTap S3 to SQS sync', {
      timestamp: syncTimestamp,
      mode: 'CSV → JSON → SQS → Redshift',
    });
    this.logger.info('═══════════════════════════════════════════════════════════════');

    try {
      // 1. List all pending CSV files in S3
      this.logger.info('📂 Step 1: Listing pending CSV files from S3...');
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

      // 2. Convert CSV files to JSON and push to SQS (streaming, no memory storage)
      this.logger.info('📝 Step 2: Converting CSV to JSON and pushing to SQS...');
      this.logger.info('   Mode: Streaming CSV → JSON → SQS (no intermediate storage)');
      const conversionStartTime = Date.now();
      const result = await this.csvToSqsService.convertCsvFilesToSqs(pendingFiles);
      const conversionDuration = Date.now() - conversionStartTime;
      
      this.logger.info('✅ CSV to SQS conversion completed', {
        durationSeconds: Math.floor(conversionDuration / 1000),
        filesProcessed: pendingFiles.length,
        totalRows: result.totalRows,
        totalMessages: result.totalMessages,
      });

      // 3. Archive processed CSV files
      this.logger.info('📦 Step 3: Archiving processed CSV files...');
      await this.clevertapS3Service.archiveFiles(pendingFiles);
      this.logger.info('✅ CSV files archived');

      const syncDuration = Date.now() - syncStartTime;
      
      this.logger.info('═══════════════════════════════════════════════════════════════');
      this.logger.info('✅ SYNC COMPLETED SUCCESSFULLY', {
        timestamp: new Date().toISOString(),
        totalFilesProcessed: pendingFiles.length,
        totalRowsPushed: result.totalRows,
        totalMessagesPushed: result.totalMessages,
        totalDurationSeconds: Math.floor(syncDuration / 1000),
        totalDurationMinutes: Math.floor(syncDuration / 60000),
        nextSyncIn: Math.floor(this.syncIntervalMs / 60000) + ' minutes',
        note: 'Data is now in SQS. SQS Consumer will load to Redshift.',
      });
      this.logger.info('═══════════════════════════════════════════════════════════════');
    } catch (error) {
      this.logger.error('═══════════════════════════════════════════════════════════════');
      this.logger.error('❌ SYNC FAILED', {
        timestamp: new Date().toISOString(),
        errorMessage: error instanceof Error ? error.message : String(error),
      });
      this.logger.error('❌ CleverTap S3 to SQS sync failed', error);
      this.logger.error('═══════════════════════════════════════════════════════════════');
    }
  }

  /**
   * Manual trigger for sync (for testing or manual operations)
   */
  async triggerSync(): Promise<void> {
    this.logger.info('🔧 Manually triggered CleverTap S3 to SQS sync');
    await this.runSync();
  }
}
