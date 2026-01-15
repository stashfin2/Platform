import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { 
  ListObjectsV2Command, 
  DeleteObjectsCommand,
  DeleteObjectCommand,
  PutObjectCommand,
  GetObjectCommand,
} from '@aws-sdk/client-s3';
import { CleverTapNonLendingS3ClientFactory } from '../config/clevertap-non-lending-s3.config';
import { LoggerService } from './logger.service';

@Service()
export class CleverTapNonLendingS3Service {
  constructor(
    @Inject() private readonly s3ClientFactory: CleverTapNonLendingS3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * List all JSON files in the clevertap-non-lending-data bucket
   * Returns array of S3 keys
   */
  async listPendingFiles(): Promise<string[]> {
    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      this.logger.info('🔍 Listing files from S3 bucket', {
        bucket: config.bucket,
        region: config.region,
      });

      const allFiles: string[] = [];
      let continuationToken: string | undefined;
      let totalObjects = 0;
      let pageCount = 0;

      do {
        pageCount++;
        const command = new ListObjectsV2Command({
          Bucket: config.bucket,
          MaxKeys: 1000,
          ContinuationToken: continuationToken,
        });

        const response = await client.send(command);
        
        if (response.Contents) {
          totalObjects += response.Contents.length;
          const jsonFiles = response.Contents
            .map(item => item.Key!)
            .filter(key => {
              if (!key) return false;
              // Filter for JSON files (including gzipped)
              const isJson = key.endsWith('.json') || key.endsWith('.json.gz');
              // Exclude manifest and jsonpath files
              const isNotMeta = !key.startsWith('manifests/') && !key.startsWith('jsonpaths/');
              return isJson && isNotMeta;
            });
          
          allFiles.push(...jsonFiles);
          
          this.logger.debug(`📄 Listed page ${pageCount}`, {
            objectsInPage: response.Contents.length,
            jsonFilesInPage: jsonFiles.length,
            totalJsonFilesSoFar: allFiles.length,
          });
        }

        continuationToken = response.NextContinuationToken;
      } while (continuationToken);

      this.logger.info(`📋 S3 listing complete`, {
        bucket: config.bucket,
        totalObjects,
        jsonFiles: allFiles.length,
        pages: pageCount,
        sampleFiles: allFiles.slice(0, 5),
      });

      if (allFiles.length === 0) {
        this.logger.warn('⚠️  No JSON files found in bucket', {
          bucket: config.bucket,
          totalObjects,
          note: 'Check if files exist and have .json or .json.gz extension',
        });
      }

      return allFiles;
    } catch (error: any) {
      const errorContext = {
        bucket: this.s3ClientFactory.getConfig().bucket,
        errorMessage: error?.message,
        errorCode: (error as any)?.Code,
        errorName: (error as any)?.name,
      };
      this.logger.error('❌ Error listing clevertap-non-lending-data S3 files', error as Error, errorContext);
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
      const batches = [];
      for (let i = 0; i < keys.length; i += 1000) {
        batches.push(keys.slice(i, i + 1000));
      }

      for (const batch of batches) {
        const command = new DeleteObjectsCommand({
          Bucket: config.bucket,
          Delete: {
            Objects: batch.map(key => ({ Key: key })),
          },
        });

        await client.send(command);
      }

      this.logger.info(`🗑️  Deleted ${keys.length} files from clevertap-non-lending-data bucket`, {
        bucket: config.bucket,
        fileCount: keys.length,
      });
    } catch (error) {
      this.logger.error('❌ Error deleting clevertap-non-lending-data S3 files', error, {
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
      await this.deleteFiles(keys);

      this.logger.info(`📦 Archived ${keys.length} files from clevertap-non-lending-data bucket`, {
        fileCount: keys.length,
      });
    } catch (error) {
      this.logger.error('❌ Error archiving clevertap-non-lending-data S3 files', error, {
        fileCount: keys.length,
      });
      throw error;
    }
  }

  /**
   * Create a JSONPath file in S3 for Redshift COPY command
   * Maps nested JSON fields to flat table columns
   * Table columns: event_date, event_ts, user_id, event_name, source_file, attributes
   */
  async createJsonPathFile(): Promise<string> {
    const client = this.s3ClientFactory.getClient();
    const config = this.s3ClientFactory.getConfig();

    // JSONPath mapping - extract fields directly
    // event_date and event_ts will get ts as integer, we'll convert in SQL
    // attributes gets full JSON
    const jsonPath = {
      jsonpaths: [
        "$.ts",                    // event_date (integer, will convert to date)
        "$.ts",                    // event_ts (integer, will convert to timestamp)
        "$.profile.identity",      // user_id
        "$.eventName",             // event_name
        "$",                       // Full JSON for attributes (SUPER)
        "$",                       // source_file placeholder
      ],
    };

    const jsonPathContent = JSON.stringify(jsonPath, null, 2);
    const jsonPathKey = `jsonpaths/clevertap-non-lending-path-${Date.now()}.json`;

    try {
      await client.send(new PutObjectCommand({
        Bucket: config.bucket,
        Key: jsonPathKey,
        Body: jsonPathContent,
        ContentType: 'application/json',
      }));

      this.logger.info('📄 Created JSONPath file', { jsonPathKey });
      return jsonPathKey;
    } catch (error) {
      this.logger.error('❌ Error creating JSONPath file', error);
      throw error;
    }
  }

  /**
   * Create a manifest file in S3 for Redshift COPY command
   * Returns the S3 key of the manifest file
   */
  async createManifestFile(files: string[]): Promise<string> {
    if (files.length === 0) {
      throw new Error('Cannot create manifest file with no files');
    }

    const client = this.s3ClientFactory.getClient();
    const config = this.s3ClientFactory.getConfig();

    // Create manifest JSON structure
    const manifest = {
      entries: files.map(file => ({
        url: `s3://${config.bucket}/${file}`,
        mandatory: true,
      })),
    };

    const manifestContent = JSON.stringify(manifest, null, 2);
    const manifestKey = `manifests/copy-${Date.now()}-${Math.random().toString(36).substring(7)}.json`;

    try {
      await client.send(new PutObjectCommand({
        Bucket: config.bucket,
        Key: manifestKey,
        Body: manifestContent,
        ContentType: 'application/json',
      }));

      this.logger.info('📄 Created manifest file', {
        manifestKey,
        fileCount: files.length,
        sizeKB: Math.floor(Buffer.byteLength(manifestContent, 'utf8') / 1024),
      });

      return manifestKey;
    } catch (error) {
      this.logger.error('❌ Error creating manifest file', error);
      throw error;
    }
  }

  /**
   * Delete a JSONPath file from S3
   */
  async deleteJsonPathFile(jsonPathKey: string): Promise<void> {
    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      await client.send(new DeleteObjectCommand({
        Bucket: config.bucket,
        Key: jsonPathKey,
      }));

      this.logger.info('🗑️  Deleted JSONPath file', { jsonPathKey });
    } catch (error) {
      this.logger.warn('⚠️  Error deleting JSONPath file (non-critical)', { jsonPathKey, error: error instanceof Error ? error.message : String(error) });
      // Don't throw - cleanup is not critical
    }
  }

  /**
   * Delete a manifest file from S3
   */
  async deleteManifestFile(manifestKey: string): Promise<void> {
    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      await client.send(new DeleteObjectCommand({
        Bucket: config.bucket,
        Key: manifestKey,
      }));

      this.logger.info('🗑️  Deleted manifest file', { manifestKey });
    } catch (error) {
      this.logger.warn('⚠️  Error deleting manifest file (non-critical)', { manifestKey, error: error instanceof Error ? error.message : String(error) });
      // Don't throw - manifest cleanup is not critical
    }
  }

  /**
   * Download and analyze a sample file to understand JSON structure
   */
  async downloadAndAnalyzeFile(key: string): Promise<any> {
    try {
      const client = this.s3ClientFactory.getClient();
      const config = this.s3ClientFactory.getConfig();

      const command = new GetObjectCommand({
        Bucket: config.bucket,
        Key: key,
      });

      const response = await client.send(command);
      
      if (!response.Body) {
        throw new Error('No body in S3 response');
      }

      // Read the stream
      const chunks: Uint8Array[] = [];
      for await (const chunk of response.Body as any) {
        chunks.push(chunk);
      }
      
      const buffer = Buffer.concat(chunks);
      
      // Check if it's gzipped
      let content: string;
      if (key.endsWith('.gz')) {
        const zlib = require('zlib');
        content = zlib.gunzipSync(buffer).toString('utf-8');
      } else {
        content = buffer.toString('utf-8');
      }

      // Parse JSON (assuming newline-delimited JSON or single JSON object)
      const lines = content.trim().split('\n').filter(line => line.trim());
      const samples = lines.slice(0, 3).map(line => {
        try {
          return JSON.parse(line);
        } catch {
          return null;
        }
      }).filter(Boolean);

      return {
        key,
        sampleCount: samples.length,
        samples,
        fullContent: content.substring(0, 1000), // First 1000 chars for inspection
      };
    } catch (error) {
      this.logger.error('❌ Error downloading file for analysis', error, { key });
      throw error;
    }
  }
}

