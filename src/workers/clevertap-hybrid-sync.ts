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

      // 2. Process each CSV: convert to events and group
      this.logger.info('📝 Step 2: Converting CSVs to structured JSONs...');
      
      const allEvents: CleverTapEvent[] = [];
      
      for (const csvFile of filesToProcess) {
        const events = await this.readCsvFile(csvFile);
        allEvents.push(...events);
        this.logger.info(`   Processed ${csvFile}: ${events.length} rows`);
      }

      this.logger.info(`✅ Total events extracted: ${allEvents.length}`);

      // 3. Write structured JSONs to S3
      this.logger.info('📤 Step 3: Writing structured JSONs to S3...');
      const s3Paths = await this.structuredJsonService.writeStructuredJson(allEvents);
      
      if (s3Paths.length === 0) {
        this.logger.warn('⚠️  No event files created (all files were profile files with null data)');
        this.logger.info('📦 Step 5: Archiving processed CSVs (profile files)...');
        await this.clevertapS3Service.archiveFiles(filesToProcess);
        this.logger.info('✅ Profile CSVs archived (no data to load)');
        
        const duration = Date.now() - syncStartTime;
        this.logger.info('═══════════════════════════════════════════════════════════════');
        this.logger.info('✅ SYNC COMPLETED (No event data found)', {
          filesProcessed: filesToProcess.length,
          eventsExtracted: allEvents.length,
          profileEventsSkipped: allEvents.length,
          jsonFilesCreated: 0,
          durationSeconds: Math.floor(duration / 1000),
        });
        this.logger.info('═══════════════════════════════════════════════════════════════');
        return;
      }
      
      this.logger.info(`✅ Created ${s3Paths.length} JSON files in structured format`);

      // 4. Push S3 paths to SQS
      this.logger.info('📨 Step 4: Pushing S3 paths to SQS...');
      const messages = s3Paths.map(path => ({
        data: {
          s3_path: path,
          row_count: allEvents.filter(e => path.includes(e.event_name)).length,
        },
        messageAttributes: {
          Source: { DataType: 'String', StringValue: 'CleverTap' },
        },
      }));

      await this.sqsService.batchPushToQueue(messages);
      this.logger.info(`✅ Pushed ${messages.length} path messages to SQS`);

      // 5. Archive processed CSVs
      this.logger.info('📦 Step 5: Archiving processed CSVs...');
      await this.clevertapS3Service.archiveFiles(filesToProcess);
      this.logger.info('✅ CSVs archived');

      const duration = Date.now() - syncStartTime;
      
      this.logger.info('═══════════════════════════════════════════════════════════════');
      this.logger.info('✅ HYBRID SYNC COMPLETED', {
        filesProcessed: filesToProcess.length,
        eventsExtracted: allEvents.length,
        jsonFilesCreated: s3Paths.length,
        durationSeconds: Math.floor(duration / 1000),
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

