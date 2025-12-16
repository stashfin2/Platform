import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { GetObjectCommand, PutObjectCommand } from '@aws-sdk/client-s3';
import { CleverTapS3ClientFactory } from '../config/clevertap.config';
import { LoggerService } from './logger.service';
import * as zlib from 'zlib';
import * as readline from 'readline';
import { Readable } from 'stream';

/**
 * CSV to JSON Converter Service
 * Converts CleverTap CSV files to JSON format for schema evolution support
 */
@Service()
export class CsvToJsonConverterService {
  private readonly jsonPrefix: string;

  constructor(
    @Inject() private readonly s3ClientFactory: CleverTapS3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {
    // Store converted JSON files in a separate prefix
    this.jsonPrefix = process.env.CLEVERTAP_JSON_PREFIX || 'clevertap-json';
  }

  /**
   * Convert CSV files to JSON format
   * Returns the S3 path where JSON files are stored
   */
  async convertCsvFilesToJson(csvFiles: string[]): Promise<string> {
    if (csvFiles.length === 0) {
      return '';
    }

    this.logger.info('🔄 Converting CSV files to JSON format', {
      fileCount: csvFiles.length,
    });

    const client = this.s3ClientFactory.getClient();
    const config = this.s3ClientFactory.getConfig();

    let totalRowsConverted = 0;
    const convertedFiles: string[] = [];

    for (const csvFile of csvFiles) {
      try {
        // Download and decompress CSV file
        const getCommand = new GetObjectCommand({
          Bucket: config.bucket,
          Key: csvFile,
        });

        const response = await client.send(getCommand);
        const csvStream = response.Body as Readable;

        // Parse CSV and convert to JSON
        const { jsonContent, rowCount, headers } = await this.parseCsvToJson(
          csvStream,
          csvFile.endsWith('.gz')
        );

        // Generate JSON filename
        const jsonFileName = `${this.jsonPrefix}/${csvFile.replace(/\.csv(\.gz)?$/, '')}_${Date.now()}.json`;

        // Upload JSON to S3
        const putCommand = new PutObjectCommand({
          Bucket: config.bucket,
          Key: jsonFileName,
          Body: jsonContent,
          ContentType: 'application/json',
        });

        await client.send(putCommand);

        totalRowsConverted += rowCount;
        convertedFiles.push(jsonFileName);

        this.logger.info(`✅ Converted CSV to JSON`, {
          csvFile,
          jsonFile: jsonFileName,
          rows: rowCount,
          columns: headers.length,
        });
      } catch (error) {
        this.logger.error(`❌ Error converting CSV file: ${csvFile}`, error);
        throw error;
      }
    }

    this.logger.info('✅ All CSV files converted to JSON', {
      totalFiles: csvFiles.length,
      totalRows: totalRowsConverted,
      convertedFiles: convertedFiles.length,
    });

    return `${config.bucket}/${this.jsonPrefix}`;
  }

  /**
   * Parse CSV stream and convert to newline-delimited JSON (NDJSON)
   * Same format as AppsFlyer uses
   */
  private async parseCsvToJson(
    csvStream: Readable,
    isCompressed: boolean
  ): Promise<{ jsonContent: string; rowCount: number; headers: string[] }> {
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
      const jsonLines: string[] = [];
      let rowCount = 0;

      rl.on('line', (line) => {
        if (isFirstLine) {
          // Parse CSV headers
          headers = this.parseCsvLine(line);
          isFirstLine = false;
        } else {
          // Parse CSV data row
          const values = this.parseCsvLine(line);
          
          // Create JSON object from headers and values
          const jsonObj: any = {};
          headers.forEach((header, index) => {
            const value = values[index] || null;
            
            // Convert empty strings to null
            jsonObj[header] = value === '' ? null : value;
          });

          // Add to JSON lines (newline-delimited JSON format)
          jsonLines.push(JSON.stringify(jsonObj));
          rowCount++;
        }
      });

      rl.on('close', () => {
        const jsonContent = jsonLines.join('\n');
        resolve({ jsonContent, rowCount, headers });
      });

      rl.on('error', (error) => {
        reject(error);
      });
    });
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
   * Clean up converted JSON files after successful load
   */
  async cleanupJsonFiles(): Promise<void> {
    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      const { ListObjectsV2Command, DeleteObjectsCommand } = await import('@aws-sdk/client-s3');

      // List all JSON files in the converted prefix
      const listCommand = new ListObjectsV2Command({
        Bucket: config.bucket,
        Prefix: this.jsonPrefix,
      });

      const response = await client.send(listCommand);
      const jsonFiles = response.Contents?.map((item) => item.Key!) || [];

      if (jsonFiles.length === 0) {
        return;
      }

      // Delete JSON files
      const deleteCommand = new DeleteObjectsCommand({
        Bucket: config.bucket,
        Delete: {
          Objects: jsonFiles.map((key) => ({ Key: key })),
        },
      });

      await client.send(deleteCommand);

      this.logger.info('🗑️  Cleaned up converted JSON files', {
        fileCount: jsonFiles.length,
      });
    } catch (error) {
      this.logger.error('❌ Error cleaning up JSON files', error);
      // Don't throw - cleanup errors shouldn't fail the sync
    }
  }
}

