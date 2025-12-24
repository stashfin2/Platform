import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import { CleverTapS3ClientFactory } from '../config/clevertap.config';
import { CleverTapSQSService } from './clevertap-sqs.service';
import { LoggerService } from './logger.service';
import * as zlib from 'zlib';
import * as readline from 'readline';
import { Readable } from 'stream';

/**
 * CleverTap CSV to SQS Service
 * Streams CSV files, converts rows to JSON, and pushes to SQS
 * Memory-efficient: processes line-by-line, no intermediate storage
 */
@Service()
export class CleverTapCsvToSqsService {
  constructor(
    @Inject() private readonly s3ClientFactory: CleverTapS3ClientFactory,
    @Inject() private readonly sqsService: CleverTapSQSService,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * Convert CSV files to JSON and push to SQS
   * Processes files in parallel with concurrency limit
   */
  async convertCsvFilesToSqs(csvFiles: string[]): Promise<{ totalRows: number; totalMessages: number }> {
    if (csvFiles.length === 0) {
      return { totalRows: 0, totalMessages: 0 };
    }

    // Reduce concurrency and batch size to avoid memory issues with large CSV files (302 columns)
    const concurrency = parseInt(process.env.CLEVERTAP_CONVERSION_CONCURRENCY || '2', 10);
    const batchSize = parseInt(process.env.CLEVERTAP_SQS_BATCH_SIZE || '10', 10); // Rows per SQS message (reduced from 100)
    
    this.logger.info('🔄 Converting CSV files to JSON and pushing to SQS', {
      fileCount: csvFiles.length,
      concurrency,
      rowsPerMessage: batchSize,
      mode: 'Streaming CSV → SQS (no memory storage)',
    });

    let totalRows = 0;
    let totalMessages = 0;

    // Process files with concurrency limit
    const processFile = async (csvFile: string): Promise<{ rows: number; messages: number }> => {
      try {
        const client = this.s3ClientFactory.getClient();
        const config = this.s3ClientFactory.getConfig();

        // Download CSV file
        const getCommand = new GetObjectCommand({
          Bucket: config.bucket,
          Key: csvFile,
        });

        const response = await client.send(getCommand);
        const csvStream = response.Body as Readable;

        // Stream CSV to JSON and push to SQS
        const result = await this.streamCsvToSqs(
          csvStream,
          csvFile.endsWith('.gz'),
          csvFile,
          batchSize
        );

        this.logger.info(`✅ Processed CSV file`, {
          csvFile,
          rows: result.rows,
          messages: result.messages,
        });

        return result;
      } catch (error) {
        this.logger.error(`❌ Error processing CSV file: ${csvFile}`, error);
        throw error;
      }
    };

    // Process files in chunks to respect concurrency limit
    for (let i = 0; i < csvFiles.length; i += concurrency) {
      const batch = csvFiles.slice(i, i + concurrency);
      const results = await Promise.allSettled(batch.map(file => processFile(file)));
      
      for (const result of results) {
        if (result.status === 'fulfilled') {
          totalRows += result.value.rows;
          totalMessages += result.value.messages;
        }
      }

      this.logger.info(`📊 Progress: ${Math.min(i + concurrency, csvFiles.length)}/${csvFiles.length} files processed`, {
        completed: Math.min(i + concurrency, csvFiles.length),
        total: csvFiles.length,
        totalRows,
        totalMessages,
      });
    }

    this.logger.info('✅ All CSV files processed and pushed to SQS', {
      totalFiles: csvFiles.length,
      totalRows,
      totalMessages,
    });

    return { totalRows, totalMessages };
  }

  /**
   * Stream CSV to JSON and push to SQS in batches
   * Memory-efficient: processes line-by-line, batches rows into SQS messages
   */
  private async streamCsvToSqs(
    csvStream: Readable,
    isCompressed: boolean,
    sourceFileName: string,
    batchSize: number
  ): Promise<{ rows: number; messages: number }> {
    return new Promise((resolve, reject) => {
      let stream: Readable = csvStream;

      // Decompress if needed
      if (isCompressed) {
        stream = csvStream.pipe(zlib.createGunzip());
      }

      const rl = readline.createInterface({
        input: stream,
        crlfDelay: Infinity,
      });

      let headers: string[] = [];
      let isFirstLine = true;
      let rowCount = 0;
      let messageCount = 0;
      const batch: any[] = [];
      const MAX_MESSAGE_SIZE = 200 * 1024; // 200KB per message (leave room under 256KB limit)
      let estimatedBatchSize = 0; // Track estimated size without stringifying

      const flushBatch = async (): Promise<void> => {
        if (batch.length === 0) return;

        try {
          // Split into smaller chunks if batch is too large
          // With 302 columns, even 5 rows can be large, so be very conservative
          const targetChunkSize = Math.min(batch.length, 5); // Max 5 rows per message for safety
          const chunks = this.chunkArray(batch, targetChunkSize);
          
          for (const chunk of chunks) {
            const messageData = {
              source: 'CleverTap',
              sourceFile: sourceFileName,
              rows: chunk,
            };
            
            await this.sqsService.batchPushToQueue([{
              data: messageData,
              messageAttributes: {
                Source: { DataType: 'String', StringValue: 'CleverTap' },
              },
            }]);
            messageCount++;
          }

          batch.length = 0; // Clear batch
          estimatedBatchSize = 0; // Reset size estimate
        } catch (error) {
          reject(error);
        }
      };

      rl.on('line', async (line: string) => {
        try {
          if (isFirstLine) {
            headers = this.parseCsvLine(line);
            isFirstLine = false;
            this.logger.info(`📋 Processing CSV with ${headers.length} columns: ${sourceFileName}`);
          } else {
            const values = this.parseCsvLine(line);
            const jsonObj = this.transformRowToJson(values, headers, sourceFileName);
            batch.push(jsonObj);
            rowCount++;

            // Estimate size (rough approximation: ~3KB per row with 302 columns)
            estimatedBatchSize += 3072; // Conservative estimate for large rows

            // Flush batch aggressively to avoid memory issues:
            // 1. Every 5 rows minimum (very conservative for 302 columns)
            // 2. When batch size reached
            // 3. When estimated size exceeds 150KB (safety margin)
            const minFlushSize = 5; // Flush every 5 rows minimum
            if (batch.length >= minFlushSize || batch.length >= batchSize || estimatedBatchSize >= (MAX_MESSAGE_SIZE * 0.75)) {
              await flushBatch();
            }
          }
        } catch (error: unknown) {
          reject(error);
        }
      });

      rl.on('close', async () => {
        try {
          // Flush remaining batch
          await flushBatch();
          resolve({ rows: rowCount, messages: messageCount });
        } catch (error: unknown) {
          reject(error);
        }
      });

      rl.on('error', (error: Error) => {
        reject(error);
      });
    });
  }

  /**
   * Transform CSV row to JSON matching CleverTap Redshift schema
   */
  private transformRowToJson(
    values: string[],
    headers: string[],
    sourceFileName: string
  ): any {
    const jsonObj: any = {};
    
    // Map CSV columns to Redshift schema
    const headerMap: { [key: string]: number } = {};
    headers.forEach((h, i) => {
      headerMap[h.toLowerCase()] = i;
    });
    
    // Extract event_ts and event_date (support event + profile files)
    let eventTs: string | null = null;
    let eventDate: string | null = null;
    const tsColumns = ['ts', 'timestamp', 'event_time', 'time', 'event_ts'];
    const createdTimeIdx = headerMap['profileattr.created_time'];
    const createdTimeVal = createdTimeIdx !== undefined ? values[createdTimeIdx] : '';
    const tsCandidates: string[] = [];
    tsColumns.forEach((col) => {
      const idx = headerMap[col];
      if (idx !== undefined && values[idx]) tsCandidates.push(values[idx]);
    });
    if (createdTimeVal) tsCandidates.push(createdTimeVal);
    for (const tsValue of tsCandidates) {
      try {
        if (/^\d{14}$/.test(tsValue)) {
          const year = tsValue.substring(0, 4);
          const month = tsValue.substring(4, 6);
          const day = tsValue.substring(6, 8);
          const hours = tsValue.substring(8, 10);
          const minutes = tsValue.substring(10, 12);
          const seconds = tsValue.substring(12, 14);
          eventDate = `${year}-${month}-${day}`;
          eventTs = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
          break;
        }
        const numericTs = parseFloat(tsValue);
        if (!isNaN(numericTs)) {
          const date = numericTs < 946684800 ? new Date(numericTs * 1000) : new Date(numericTs);
          if (!isNaN(date.getTime())) {
            const year = date.getFullYear();
            const month = String(date.getMonth() + 1).padStart(2, '0');
            const day = String(date.getDate()).padStart(2, '0');
            const hours = String(date.getHours()).padStart(2, '0');
            const minutes = String(date.getMinutes()).padStart(2, '0');
            const seconds = String(date.getSeconds()).padStart(2, '0');
            eventDate = `${year}-${month}-${day}`;
            eventTs = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
            break;
          }
        }
        const date = new Date(tsValue);
        if (!isNaN(date.getTime())) {
          const year = date.getFullYear();
          const month = String(date.getMonth() + 1).padStart(2, '0');
          const day = String(date.getDate()).padStart(2, '0');
          const hours = String(date.getHours()).padStart(2, '0');
          const minutes = String(date.getMinutes()).padStart(2, '0');
          const seconds = String(date.getSeconds()).padStart(2, '0');
          eventDate = `${year}-${month}-${day}`;
          eventTs = `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
          break;
        }
      } catch {
        // continue
      }
    }
    
    // Fallback to current date/time if not found
    if (!eventTs || !eventDate) {
      const now = new Date();
      const year = now.getFullYear();
      const month = String(now.getMonth() + 1).padStart(2, '0');
      const day = String(now.getDate()).padStart(2, '0');
      const hours = String(now.getHours()).padStart(2, '0');
      const minutes = String(now.getMinutes()).padStart(2, '0');
      const seconds = String(now.getSeconds()).padStart(2, '0');
      
      eventDate = eventDate || `${year}-${month}-${day}`;
      eventTs = eventTs || `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    }
    
    jsonObj.event_date = eventDate;
    jsonObj.event_ts = eventTs;
    
    // Extract user_id based on file type
    let userId: string | null = null;
    
    // Detect if this is an event file (has ts/eventName columns) or profile file
    const isEventFile = headerMap['ts'] !== undefined || 
                       headerMap['eventname'] !== undefined || 
                       headerMap['event_name'] !== undefined;
    
    if (isEventFile) {
      // Event files: use profile.identity
      const profileIdentityIdx = headerMap['profile.identity'];
      if (profileIdentityIdx !== undefined && values[profileIdentityIdx] && values[profileIdentityIdx].trim() !== '') {
        userId = values[profileIdentityIdx].trim();
      }
    } else {
      // Profile files: priority order: identity → phone → cleverTapId
      const identityIdx = headerMap['identity'];
      if (identityIdx !== undefined && values[identityIdx] && values[identityIdx].trim() !== '') {
        userId = values[identityIdx].trim();
      }
      
      if (!userId) {
        const phoneIdx = headerMap['phone'];
        if (phoneIdx !== undefined && values[phoneIdx] && values[phoneIdx].trim() !== '') {
          userId = values[phoneIdx].trim();
        }
      }
      
      if (!userId) {
        const cleverTapIdIdx = headerMap['clevertapid'];
        if (cleverTapIdIdx !== undefined && values[cleverTapIdIdx] && values[cleverTapIdIdx].trim() !== '') {
          userId = values[cleverTapIdIdx].trim();
        }
      }
    }
    
    jsonObj.user_id = userId;
    
    // Extract event_name; fallback for profile files
    let eventName: string | null = null;
    const eventNameColumns = ['event_name', 'eventname', 'event', 'name', 'Event Name', 'eventName'];
    for (const col of eventNameColumns) {
      const idx = headerMap[col];
      if (idx !== undefined && values[idx]) {
        eventName = values[idx];
        if (eventName && eventName !== '') break;
      }
    }
    // Profile fallback: if still missing, treat as profile update
    if (!eventName || eventName === '') {
      // Check if filename hints at event type
      const fileName = sourceFileName.split('/').pop() || '';
      const match = fileName.match(/([A-Z][a-zA-Z_]+)/);
      eventName = match ? match[1] : 'profile_update';
    }
    jsonObj.event_name = eventName;
    
    jsonObj.source_file = sourceFileName;
    
    // All other columns go into attributes
    const attributes: any = {};
    headers.forEach((header, index) => {
      const headerLower = header.toLowerCase();
      if (
        headerLower !== 'event_date' &&
        headerLower !== 'eventdate' &&
        headerLower !== 'ts' &&
        headerLower !== 'timestamp' &&
        headerLower !== 'event_time' &&
        headerLower !== 'event_ts' &&
        headerLower !== 'time' &&
        headerLower !== 'user_id' &&
        headerLower !== 'userid' &&
        headerLower !== 'identity' &&
        headerLower !== 'user' &&
        headerLower !== 'customer_user_id' &&
        headerLower !== 'profile.identity' &&
        headerLower !== 'event_name' &&
        headerLower !== 'eventname' &&
        headerLower !== 'event' &&
        headerLower !== 'name' &&
        headerLower !== 'eventname'
      ) {
        const value = values[index] || null;
        attributes[header] = value === '' ? null : value;
      }
    });
    jsonObj.attributes = attributes;

    return jsonObj;
  }

  /**
   * Parse CSV line handling quoted fields
   */
  private parseCsvLine(line: string): string[] {
    const values: string[] = [];
    let currentValue = '';
    let insideQuotes = false;

    for (let i = 0; i < line.length; i++) {
      const char = line[i];
      const nextChar = line[i + 1];

      if (char === '"') {
        if (insideQuotes && nextChar === '"') {
          // Escaped quote
          currentValue += '"';
          i++; // Skip next quote
        } else {
          // Toggle quote state
          insideQuotes = !insideQuotes;
        }
      } else if (char === ',' && !insideQuotes) {
        // End of field
        values.push(currentValue.trim());
        currentValue = '';
      } else {
        currentValue += char;
      }
    }

    // Add last value
    values.push(currentValue.trim());

    return values;
  }

  /**
   * Utility to chunk array into smaller arrays
   */
  private chunkArray<T>(array: T[], chunkSize: number): T[][] {
    const chunks: T[][] = [];
    for (let i = 0; i < array.length; i += chunkSize) {
      chunks.push(array.slice(i, i + chunkSize));
    }
    return chunks;
  }
}

