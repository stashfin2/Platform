import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { CleverTapS3Service } from '../services/clevertap-s3.service';
import { CleverTapCsvToSqsService } from '../services/clevertap-csv-to-sqs.service';
import { CleverTapStructuredJsonService } from '../services/clevertap-structured-json.service';
import { CleverTapSQSService } from '../services/clevertap-sqs.service';
import { LoggerService } from '../services/logger.service';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { CleverTapS3ClientFactory } from '../config/clevertap.config';
import * as zlib from 'zlib';
import * as readline from 'readline';
import { Readable } from 'stream';
import { CleverTapEvent } from '../services/clevertap-redshift.service';

/**
 * CleverTap Hybrid Sync Worker
 * Hybrid approach: CSV → Structured JSON (S3) → SQS (S3 paths) → Consumer → COPY → Redshift
 * Benefits: Small SQS messages, structured storage, SQS reliability
 */
@Service()
export class CleverTapHybridSync {
  private isRunning: boolean = false;
  private syncInterval: NodeJS.Timeout | null = null;
  private readonly syncIntervalMs: number;

  constructor(
    @Inject() private readonly clevertapS3Service: CleverTapS3Service,
    @Inject() private readonly s3ClientFactory: CleverTapS3ClientFactory,
    @Inject() private readonly csvToSqsService: CleverTapCsvToSqsService,
    @Inject() private readonly structuredJsonService: CleverTapStructuredJsonService,
    @Inject() private readonly sqsService: CleverTapSQSService,
    @Inject() private readonly logger: LoggerService
  ) {
    this.syncIntervalMs = parseInt(process.env.CLEVERTAP_SYNC_INTERVAL_MS || '1800000', 10);
  }

  async start(): Promise<void> {
    this.isRunning = true;
    this.logger.info('🚀 CleverTap Hybrid Sync started', {
      syncIntervalMs: this.syncIntervalMs,
      mode: 'CSV → Structured S3 JSON → SQS (paths) → Redshift',
    });

    await this.runSync();

    this.syncInterval = setInterval(() => {
      this.runSync();
    }, this.syncIntervalMs);
  }

  stop(): void {
    this.isRunning = false;
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = null;
    }
    this.logger.info('⛔ CleverTap Hybrid Sync stopped');
  }

  private async runSync(): Promise<void> {
    if (!this.isRunning) return;

    const syncStartTime = Date.now();
    
    this.logger.info('═══════════════════════════════════════════════════════════════');
    this.logger.info('🔄 Starting CleverTap Hybrid Sync', {
      timestamp: new Date().toISOString(),
      mode: 'Structured S3 + SQS paths',
    });
    this.logger.info('═══════════════════════════════════════════════════════════════');

    try {
      // 1. List pending CSV files
      this.logger.info('📂 Step 1: Listing pending CSV files...');
      const pendingFiles = await this.clevertapS3Service.listPendingFiles();

      if (pendingFiles.length === 0) {
        this.logger.info('✅ No files to sync');
        return;
      }

      // Test mode: limit files for local testing
      const testLimit = parseInt(process.env.CLEVERTAP_TEST_FILE_LIMIT || '0', 10);
      const filesToProcess = testLimit > 0 ? pendingFiles.slice(0, testLimit) : pendingFiles;
      
      this.logger.info(`📋 Processing ${filesToProcess.length} files${testLimit > 0 ? ' (TEST MODE)' : ''}`);

      // 2. Process each CSV file ONE AT A TIME (memory-efficient)
      this.logger.info('📝 Step 2: Processing CSVs one at a time (memory-efficient)...');
      
      const allS3Paths: string[] = [];
      const processedFiles: string[] = [];
      let totalEventsProcessed = 0;
      let totalJsonFilesCreated = 0;
      
      for (const csvFile of filesToProcess) {
        try {
          this.logger.info(`   Processing file: ${csvFile}`);
          
          // Read CSV file into events (only this file in memory)
          const events = await this.readCsvFile(csvFile);
          this.logger.info(`   ├─ Read ${events.length} events from CSV`);
          
          if (events.length === 0) {
            this.logger.info(`   └─ Skipping (no events)`);
            processedFiles.push(csvFile);
            continue;
          }
          
          // Write structured JSON to S3 (only this file's data)
          const s3Paths = await this.structuredJsonService.writeStructuredJson(events);
          this.logger.info(`   ├─ Created ${s3Paths.length} JSON files`);
          
          if (s3Paths.length > 0) {
            // Push S3 paths to SQS immediately
            const messages = s3Paths.map(path => ({
              data: {
                s3_path: path,
                source_file: csvFile,
                row_count: events.length,
              },
              messageAttributes: {
                Source: { DataType: 'String', StringValue: 'CleverTap' },
              },
            }));
            
            await this.sqsService.batchPushToQueue(messages);
            this.logger.info(`   ├─ Pushed ${messages.length} messages to SQS`);
            
            allS3Paths.push(...s3Paths);
            totalJsonFilesCreated += s3Paths.length;
          }
          
          totalEventsProcessed += events.length;
          processedFiles.push(csvFile);
          
          this.logger.info(`   └─ ✅ File completed (${events.length} events)`);
          
          // Clear memory after each file (help GC)
          if (global.gc) {
            global.gc();
          }
          
        } catch (error) {
          this.logger.error(`   └─ ❌ Error processing ${csvFile}`, error);
          // Continue with next file instead of failing entire sync
        }
      }

      if (processedFiles.length === 0) {
        this.logger.warn('⚠️  No files were processed successfully');
        return;
      }

      this.logger.info(`✅ All files processed: ${processedFiles.length}/${filesToProcess.length}`);
      this.logger.info(`   Total events: ${totalEventsProcessed}`);
      this.logger.info(`   Total JSON files: ${totalJsonFilesCreated}`);
      this.logger.info(`   Total SQS messages: ${allS3Paths.length}`);

      // 3. Archive processed CSVs (only the ones that succeeded)
      this.logger.info('📦 Step 3: Archiving successfully processed CSVs...');
      await this.clevertapS3Service.archiveFiles(processedFiles);
      this.logger.info('✅ CSVs archived');

      const duration = Date.now() - syncStartTime;
      
      this.logger.info('═══════════════════════════════════════════════════════════════');
      this.logger.info('✅ HYBRID SYNC COMPLETED', {
        filesProcessed: processedFiles.length,
        filesTotal: filesToProcess.length,
        eventsExtracted: totalEventsProcessed,
        jsonFilesCreated: totalJsonFilesCreated,
        sqsMessagesCreated: allS3Paths.length,
        durationSeconds: Math.floor(duration / 1000),
        memoryEfficient: true,
      });
      this.logger.info('═══════════════════════════════════════════════════════════════');
    } catch (error) {
      this.logger.error('❌ Hybrid sync failed', error);
    }
  }

  /**
   * Read CSV file and convert to CleverTapEvent array
   * In test mode, limits the number of rows to prevent memory issues
   */
  private async readCsvFile(csvFile: string): Promise<CleverTapEvent[]> {
    const client = this.s3ClientFactory.getClient();
    const config = this.s3ClientFactory.getConfig();

    // Test mode: limit number of rows to process
    const testRowLimit = parseInt(process.env.CLEVERTAP_TEST_ROW_LIMIT || '0', 10);

    const response = await client.send(new GetObjectCommand({
      Bucket: config.bucket,
      Key: csvFile,
    }));

    let stream: Readable = response.Body as Readable;
    if (csvFile.endsWith('.gz')) {
      stream = stream.pipe(zlib.createGunzip());
    }

    return new Promise((resolve, reject) => {
      const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
      const events: CleverTapEvent[] = [];
      let headers: string[] = [];
      let first = true;
      let rowCount = 0;

      rl.on('line', (line: string) => {
        try {
          if (first) {
            headers = (this.csvToSqsService as any).parseCsvLine(line);
            first = false;
          } else {
            // Check if we've reached the test row limit
            if (testRowLimit > 0 && rowCount >= testRowLimit) {
              rl.close();
              return;
            }

            const values = (this.csvToSqsService as any).parseCsvLine(line);
            const event = (this.csvToSqsService as any).transformRowToJson(values, headers, csvFile);
            events.push(event);
            rowCount++;
          }
        } catch (error) {
          reject(error);
        }
      });

      rl.on('close', () => {
        if (testRowLimit > 0) {
          this.logger.info(`📊 Test mode: Limited to ${rowCount} rows from CSV`, {
            csvFile,
            testRowLimit,
            actualRows: rowCount,
          });
        }
        resolve(events);
      });
      rl.on('error', (error: Error) => reject(error));
    });
  }

  async triggerSync(): Promise<void> {
    this.logger.info('🔧 Manually triggered hybrid sync');
    await this.runSync();
  }
}

