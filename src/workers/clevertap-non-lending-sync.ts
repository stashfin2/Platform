import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { ExecuteStatementCommand, DescribeStatementCommand } from '@aws-sdk/client-redshift-data';
import { CleverTapNonLendingS3Service } from '../services/clevertap-non-lending-s3.service';
import { CleverTapNonLendingRedshiftService } from '../services/clevertap-non-lending-redshift.service';
import { RedshiftClientFactory } from '../config/redshift.config';
import { CleverTapNonLendingS3ClientFactory } from '../config/clevertap-non-lending-s3.config';
import { LoggerService } from '../services/logger.service';
import * as fs from 'fs';
import * as path from 'path';

/**
 * CleverTap Non-Lending Data S3 to Redshift Sync Worker
 * Continuously syncs JSON data from clevertap-non-lending-data bucket to Redshift
 */
@Service()
export class CleverTapNonLendingSync {
  private isRunning: boolean = false;
  private syncInterval: NodeJS.Timeout | null = null;
  private tableCreated: boolean = false;
  private fileAnalyzed: boolean = false;
  private totalFilesToProcess: number = 0;
  private filesProcessed: number = 0;
  private allFilesProcessed: boolean = false;
  private readonly progressFilePath: string;
  private processedFileKeys: Set<string> = new Set();

  constructor(
    @Inject() private readonly s3Service: CleverTapNonLendingS3Service,
    @Inject() private readonly redshiftService: CleverTapNonLendingRedshiftService,
    @Inject() private readonly redshiftClientFactory: RedshiftClientFactory,
    @Inject() private readonly s3ClientFactory: CleverTapNonLendingS3ClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {
    // Store progress in a file to enable resume on restart
    this.progressFilePath = path.join(process.cwd(), '.clevertap-non-lending-progress.json');
    this.loadProgress();
  }

  /**
   * Load progress from file to enable resume
   */
  private loadProgress(): void {
    try {
      if (fs.existsSync(this.progressFilePath)) {
        const progressData = JSON.parse(fs.readFileSync(this.progressFilePath, 'utf-8'));
        this.filesProcessed = progressData.filesProcessed || 0;
        this.totalFilesToProcess = progressData.totalFilesToProcess || 0;
        this.processedFileKeys = new Set(progressData.processedFileKeys || []);
        this.logger.info('📂 Loaded progress from file', {
          filesProcessed: this.filesProcessed,
          totalFilesToProcess: this.totalFilesToProcess,
          processedFileKeysCount: this.processedFileKeys.size,
          progressFile: this.progressFilePath,
        });
      }
    } catch (error) {
      this.logger.warn('⚠️  Could not load progress file, starting fresh', error as Error);
    }
  }

  /**
   * Save progress to file
   */
  private saveProgress(): void {
    try {
      const progressData = {
        filesProcessed: this.filesProcessed,
        totalFilesToProcess: this.totalFilesToProcess,
        processedFileKeys: Array.from(this.processedFileKeys),
        lastUpdated: new Date().toISOString(),
      };
      fs.writeFileSync(this.progressFilePath, JSON.stringify(progressData, null, 2), 'utf-8');
    } catch (error) {
      this.logger.warn('⚠️  Could not save progress file', error as Error);
    }
  }

  /**
   * Start the sync worker
   * Processes all files once and stops when complete
   */
  async start(): Promise<void> {
    this.isRunning = true;
    this.logger.info('🚀 CleverTap Non-Lending Data sync worker started', {
      mode: 'Process all files once and stop',
    });

    // Create table if not exists (only once)
    if (!this.tableCreated) {
      try {
        await this.redshiftService.createTableIfNotExists();
        this.tableCreated = true;
      } catch (error) {
        this.logger.error('❌ Failed to create table, will retry on next sync', error);
      }
    }

    // Run sync once - it will process all files and stop
    await this.runSync();
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
    this.logger.info('⛔ CleverTap Non-Lending Data sync worker stopped');
  }

  /**
   * Run a single sync operation
   * Processes all files and stops when complete
   */
  private async runSync(): Promise<void> {
    if (!this.isRunning || this.allFilesProcessed) {
      return;
    }

    const syncStartTime = Date.now();
    this.logger.info('🔄 Starting CleverTap Non-Lending Data sync...');

    try {
      // Ensure table exists
      if (!this.tableCreated) {
        await this.redshiftService.createTableIfNotExists();
        this.tableCreated = true;
      }

      // 1. List all pending files in S3
      this.logger.info('🔍 Starting to list files from S3...');
      let allFiles: string[] = [];
      try {
        allFiles = await this.s3Service.listPendingFiles();
        this.logger.info(`📋 Listing complete: Found ${allFiles.length} total files in S3`);
      } catch (error) {
        this.logger.error('❌ Failed to list files from S3', error as Error);
        throw error;
      }

      // Set total files count on first run
      if (this.totalFilesToProcess === 0 && allFiles.length > 0) {
        this.totalFilesToProcess = allFiles.length;
        this.logger.info(`📊 Total files to process: ${this.totalFilesToProcess}`);
      }

      // 2. Filter out already processed files using the tracking set
      const pendingFiles = allFiles.filter(file => !this.processedFileKeys.has(file));
      
      if (pendingFiles.length === 0 && allFiles.length > 0) {
        this.logger.info('✅ All files already processed!', {
          totalFiles: allFiles.length,
          processedFiles: this.processedFileKeys.size,
        });
        this.allFilesProcessed = true;
        this.stop();
        return;
      }

      if (pendingFiles.length < allFiles.length) {
        this.logger.info('🔄 Resuming sync', {
          filesAlreadyProcessed: this.processedFileKeys.size,
          filesRemaining: pendingFiles.length,
          totalFiles: allFiles.length,
        });
      }

      if (pendingFiles.length === 0) {
        if (this.filesProcessed > 0) {
          this.logger.info('✅ All files processed!', {
            totalFilesProcessed: this.filesProcessed,
            totalFilesExpected: this.totalFilesToProcess,
            match: this.filesProcessed === this.totalFilesToProcess ? '✅ MATCH' : '⚠️ MISMATCH',
          });
          this.allFilesProcessed = true;
          this.stop();
        } else {
          this.logger.warn('⚠️  No files to sync - check S3 bucket and credentials', {
            bucket: this.s3ClientFactory.getConfig().bucket,
            region: this.s3ClientFactory.getConfig().region,
          });
        }
        return;
      }

      this.logger.info(`✅ Found ${pendingFiles.length} files to sync`, {
        filesProcessed: this.filesProcessed,
        filesRemaining: pendingFiles.length,
        totalFilesExpected: this.totalFilesToProcess,
        sampleFiles: pendingFiles.slice(0, 5),
      });

      // Analyze first file to understand JSON structure (only once)
      if (pendingFiles.length > 0 && !this.fileAnalyzed) {
        try {
          this.logger.info('🔍 Analyzing sample file to understand JSON structure...');
          const analysis = await this.s3Service.downloadAndAnalyzeFile(pendingFiles[0]);
          this.logger.info('📊 File analysis complete', {
            key: analysis.key,
            sampleCount: analysis.sampleCount,
            sampleKeys: analysis.samples[0] ? Object.keys(analysis.samples[0]) : [],
            firstSample: JSON.stringify(analysis.samples[0], null, 2).substring(0, 2000),
          });
          this.fileAnalyzed = true;
        } catch (error) {
          this.logger.warn('⚠️  Could not analyze sample file, continuing anyway', error as Error);
        }
      }

      // 2. Use Redshift COPY command to load data from S3
      // Optimize for speed: maximum batch size and parallel processing
      const batchSize = parseInt(process.env.CLEVERTAP_NON_LENDING_BATCH_SIZE || '1000', 10); // Default: 1000 files per batch (max)
      const maxParallelBatches = parseInt(process.env.CLEVERTAP_NON_LENDING_MAX_PARALLEL || '10', 10); // Process 5 batches in parallel
      
      let processedCount = 0;
      const totalBatches = Math.ceil(pendingFiles.length / batchSize);
      
      this.logger.info('🚀 Starting parallel batch processing', {
        totalFiles: pendingFiles.length,
        batchSize,
        totalBatches,
        maxParallelBatches,
        estimatedTimeMinutes: Math.ceil((totalBatches / maxParallelBatches) * 2), // ~2 min per batch
      });

      // Process batches in parallel chunks
      for (let i = 0; i < pendingFiles.length; i += batchSize * maxParallelBatches) {
        const parallelBatches: Array<{ batch: string[], batchNumber: number }> = [];
        
        // Create parallel batch group
        for (let j = 0; j < maxParallelBatches && (i + j * batchSize) < pendingFiles.length; j++) {
          const batchStart = i + j * batchSize;
          const batch = pendingFiles.slice(batchStart, batchStart + batchSize);
          if (batch.length > 0) {
            parallelBatches.push({
              batch,
              batchNumber: Math.floor(batchStart / batchSize) + 1,
            });
          }
        }

        if (parallelBatches.length === 0) break;

        this.logger.info(`📦 Processing ${parallelBatches.length} batches in parallel`, {
          batchRange: `${parallelBatches[0].batchNumber}-${parallelBatches[parallelBatches.length - 1].batchNumber}`,
          totalBatches,
          filesInThisGroup: parallelBatches.reduce((sum, pb) => sum + pb.batch.length, 0),
        });

        // Process all batches in this group in parallel
        const batchPromises = parallelBatches.map(async ({ batch, batchNumber }) => {
          let manifestKey: string = '';
          try {
            // COPY, transform, and insert all happen in one transaction
            const { statementId, manifestKey: mk } = await this.copyFromS3ToRedshift(batch);
            manifestKey = mk;

            // Wait for the entire transaction to complete
            await this.waitForStatementCompletion(statementId);

            // Mark files as processed
            batch.forEach(file => this.processedFileKeys.add(file));
            
            // Clean up manifest file after successful COPY
            await this.s3Service.deleteManifestFile(manifestKey);

            this.logger.info(`✅ Completed batch ${batchNumber}/${totalBatches}`, {
              batchNumber,
              filesInBatch: batch.length,
            });

            return { success: true, batchNumber, fileCount: batch.length };
          } catch (error) {
            // Clean up manifest file even on error
            if (manifestKey) {
              await this.s3Service.deleteManifestFile(manifestKey);
            }
            this.logger.error(`❌ Batch ${batchNumber} failed`, error as Error);
            return { success: false, batchNumber, fileCount: batch.length, error };
          }
        });

        // Wait for all batches in this parallel group to complete
        const results = await Promise.allSettled(batchPromises);
        
        // Count successful batches
        let groupProcessed = 0;
        results.forEach((result, idx) => {
          if (result.status === 'fulfilled' && result.value.success) {
            groupProcessed += result.value.fileCount;
          } else {
            const batchNum = parallelBatches[idx]?.batchNumber || 'unknown';
            this.logger.error(`❌ Batch ${batchNum} failed`, 
              result.status === 'rejected' ? result.reason : result.value.error);
          }
        });

        processedCount += groupProcessed;
        this.filesProcessed += groupProcessed;
        
        // Save progress after each parallel group
        this.saveProgress();

        this.logger.info(`✅ Completed parallel batch group`, {
          batchesProcessed: results.filter(r => r.status === 'fulfilled' && r.value.success).length,
          batchesTotal: parallelBatches.length,
          filesProcessed: groupProcessed,
          totalProcessed: this.filesProcessed,
          totalRemaining: this.totalFilesToProcess - this.filesProcessed,
          overallProgressPercent: this.totalFilesToProcess > 0 ? ((this.filesProcessed / this.totalFilesToProcess) * 100).toFixed(1) : '0',
        });
      }

      const syncDuration = Date.now() - syncStartTime;
      this.logger.info('✅ CleverTap Non-Lending Data sync batch completed successfully', {
        filesInThisBatch: pendingFiles.length,
        totalFilesProcessed: this.filesProcessed,
        totalFilesExpected: this.totalFilesToProcess,
        durationMs: syncDuration,
        durationSeconds: Math.floor(syncDuration / 1000),
        durationMinutes: (syncDuration / 60000).toFixed(2),
      });

      // Check if all files are processed
      if (this.filesProcessed >= this.totalFilesToProcess) {
        this.logger.info('🎉 All files processed!', {
          totalFilesProcessed: this.filesProcessed,
          totalFilesExpected: this.totalFilesToProcess,
          match: this.filesProcessed === this.totalFilesToProcess ? '✅ MATCH' : '⚠️ MISMATCH',
        });
        this.allFilesProcessed = true;
        this.stop();
      } else {
        // Continue processing remaining files
        this.logger.info('🔄 Continuing to process remaining files...', {
          filesProcessed: this.filesProcessed,
          filesRemaining: this.totalFilesToProcess - this.filesProcessed,
        });
        // Recursively call runSync to process remaining files
        await this.runSync();
      }
    } catch (error) {
      this.logger.error('❌ CleverTap Non-Lending Data sync failed', error);
      // Don't delete files on error - they'll be retried in next sync
    }
  }

  /**
   * Execute Redshift COPY command to load data from S3 to staging table, then transform and insert into final table
   * Uses staging table to load raw JSON, then transforms and inserts into final table in one transaction
   * Uses JSON 'noshred' to load entire JSON into SUPER column, then extracts fields using json_serialize + json_extract_path_text
   * Returns statementId and manifestKey for cleanup
   */
  private async copyFromS3ToRedshift(files: string[]): Promise<{ statementId: string; manifestKey: string }> {
    const client = this.redshiftClientFactory.getClient();
    const redshiftConfig = this.redshiftClientFactory.getConfig();
    const s3Config = this.s3ClientFactory.getConfig();

    // Redshift supports up to 1000 files per COPY command
    if (files.length > 1000) {
      throw new Error(`Too many files in batch: ${files.length}. Maximum is 1000 per COPY command.`);
    }

    // Create manifest file in S3 to avoid query string size limits
    const manifestKey = await this.s3Service.createManifestFile(files);
    const manifestPath = `s3://${s3Config.bucket}/${manifestKey}`;

    // Use staging table
    const stagingTableName = this.redshiftService.getFullStagingTableName();
    const tableName = this.redshiftService.getFullTableName();
    
    // Step 1: Truncate staging table
    const truncateStagingSql = `TRUNCATE TABLE ${stagingTableName};`;

    // Step 2: Load raw JSON into staging table using JSON 'noshred'
    const copySql = process.env.REDSHIFT_IAM_ROLE
      ? `
        COPY ${stagingTableName} (raw_data)
        FROM '${manifestPath}'
        IAM_ROLE '${process.env.REDSHIFT_IAM_ROLE}'
        MANIFEST
        JSON 'noshred'
        GZIP
        TRUNCATECOLUMNS
        BLANKSASNULL
        EMPTYASNULL
        MAXERROR 10
        COMPUPDATE OFF
        STATUPDATE OFF;
      `.trim()
      : `
        COPY ${stagingTableName} (raw_data)
        FROM '${manifestPath}'
        ACCESS_KEY_ID '${s3Config.accessKeyId}'
        SECRET_ACCESS_KEY '${s3Config.secretAccessKey}'
        MANIFEST
        JSON 'noshred'
        GZIP
        TRUNCATECOLUMNS
        BLANKSASNULL
        EMPTYASNULL
        MAXERROR 10
        COMPUPDATE OFF
        STATUPDATE OFF;
      `.trim();

    // Step 3: Transform and insert into final table
    const transformAndInsertSql = `
      INSERT INTO ${tableName} (
        event_date,
        event_ts,
        user_id,
        event_name,
        source_file,
        attributes
      )
      SELECT
        DATE(TO_TIMESTAMP(CAST(raw_data.ts AS VARCHAR), 'YYYYMMDDHH24MISS')) AS event_date,
        TO_TIMESTAMP(CAST(raw_data.ts AS VARCHAR), 'YYYYMMDDHH24MISS') AS event_ts,
        COALESCE(
          -- First, check if profile.identity is numeric (pure digits, no +, no @)
          CASE 
            WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'identity') ~ '^[0-9]+$'
              AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'identity') NOT LIKE '+%'
              AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'identity') NOT LIKE '%@%'
            THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'identity')
            ELSE NULL
          END,
          -- Then check all_identities array - check indices 0-9 for numeric values
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '0') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '0') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '0') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '0')
               ELSE NULL END,
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '1') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '1') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '1') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '1')
               ELSE NULL END,
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '2') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '2') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '2') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '2')
               ELSE NULL END,
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '3') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '3') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '3') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '3')
               ELSE NULL END,
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '4') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '4') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '4') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '4')
               ELSE NULL END,
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '5') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '5') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '5') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '5')
               ELSE NULL END,
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '6') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '6') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '6') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '6')
               ELSE NULL END,
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '7') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '7') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '7') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '7')
               ELSE NULL END,
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '8') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '8') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '8') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '8')
               ELSE NULL END,
          CASE WHEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '9') ~ '^[0-9]+$'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '9') NOT LIKE '+%'
                AND JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '9') NOT LIKE '%@%'
               THEN JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'profile', 'all_identities', '9')
               ELSE NULL END,
          -- If no numeric ID found, return NULL instead of email/phone
          NULL
        ) AS user_id,
        JSON_EXTRACT_PATH_TEXT(JSON_SERIALIZE(raw_data), 'eventName') AS event_name,
        '' AS source_file,
        raw_data AS attributes
      FROM ${stagingTableName}
      WHERE raw_data IS NOT NULL
        AND raw_data.ts IS NOT NULL;
    `.trim();

    // Step 4: Clear staging table
    const clearStagingSql = `TRUNCATE TABLE ${stagingTableName};`;

    // Combine all steps into one transaction
    const combinedSql = `
      BEGIN;
      ${truncateStagingSql}
      ${copySql}
      ${transformAndInsertSql}
      ${clearStagingSql}
      COMMIT;
    `.trim();

    this.logger.info('📥 Executing Redshift COPY command with transformation', {
      stagingTable: stagingTableName,
      finalTable: tableName,
      bucket: s3Config.bucket,
      fileCount: files.length,
      sampleFiles: files.slice(0, 3),
      manifestKey,
    });

    try {
      const command = new ExecuteStatementCommand({
        ClusterIdentifier: redshiftConfig.clusterIdentifier,
        Database: redshiftConfig.database,
        DbUser: redshiftConfig.dbUser,
        Sql: combinedSql,
      });

      const response = await client.send(command);
      const statementId = response.Id || '';

      this.logger.info('📨 Redshift COPY command submitted', {
        statementId,
        stagingTable: stagingTableName,
        finalTable: tableName,
        fileCount: files.length,
        manifestKey,
      });

      return { statementId, manifestKey };
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
    const startTime = Date.now();
    const maxRetries = 300; // 10 minutes (300 * 2 seconds)

    for (let i = 0; i < maxRetries; i++) {
      try {
        const command = new DescribeStatementCommand({ Id: statementId });
        const response = await client.send(command);
        
        const status = response.Status;
        
        if (i > 0 && i % 15 === 0) {
          // Log progress every 15 attempts (every 30 seconds)
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
          
          // Check if rows were actually loaded (for COPY, ResultRows is often 0 even when successful)
          // But if it's 0, we should verify data was loaded by checking the table
          this.logger.info('✅ Redshift COPY completed successfully', {
            statementId,
            durationMs: duration,
            durationSeconds: Math.floor(duration / 1000),
            durationMinutes: (duration / 60000).toFixed(2),
            attempts: i + 1,
            rowsAffected,
            note: rowsAffected === 0 ? 'ResultRows=0 is common for COPY. Verify data in table.' : 'Rows loaded successfully',
          });
          
          if (rowsAffected === 0) {
            this.logger.warn('⚠️  COPY completed but ResultRows=0. This is normal for COPY commands, but verify data was loaded.', {
              statementId,
            });
          }
          
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
    this.logger.info('🔧 Manually triggered CleverTap Non-Lending Data sync');
    await this.runSync();
  }
}

