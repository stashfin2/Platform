import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { SQSService } from '../services/sqs.service';
import { RedshiftService } from '../services/redshift.service';
import { LoggerService } from '../services/logger.service';


@Service()
export class SQSConsumer {
  private isRunning: boolean = false;
  private workerId: string;
  private messagesProcessed: number = 0;
  private messagesFailed: number = 0;
  private startTime: number = Date.now();

  constructor(
    @Inject() private readonly sqsService: SQSService,
    @Inject() private readonly redshiftService: RedshiftService,
    @Inject() private readonly logger: LoggerService
  ) {
    this.workerId = process.env.WORKER_ID || `worker-${process.pid}`;
  }

  /**
   * Start the SQS consumer
   */
  async start(): Promise<void> {
    this.isRunning = true;
    this.startTime = Date.now();
    this.logger.info(`🚀 SQS Consumer started [${this.workerId}]`, {
      workerId: this.workerId,
      pid: process.pid,
    });

    // Log throughput metrics every 60 seconds
    this.startMetricsLogging();

    // Start polling (non-blocking - runs in background)
    this.poll();
  }

  /**
   * Stop the SQS consumer
   */
  stop(): void {
    this.isRunning = false;
    this.logger.info(`⛔ SQS Consumer stopped [${this.workerId}]`, {
      workerId: this.workerId,
      messagesProcessed: this.messagesProcessed,
      messagesFailed: this.messagesFailed,
    });
  }

  /**
   * Poll SQS for messages continuously
   * Uses long polling - NO artificial delays between polls
   * Accumulates messages into larger batches for optimal Redshift performance
   */
  private async poll(): Promise<void> {
    // Target batch size (configurable via env)
    const targetBatchSize = parseInt(process.env.TARGET_BATCH_SIZE || '100', 10);
    const maxWaitTimeMs = parseInt(process.env.MAX_BATCH_WAIT_MS || '5000', 10); // Max 5s wait
    
    let accumulatedMessages: Array<{data: any, receiptHandle: string, messageId: string}> = [];
    let lastBatchTime = Date.now();

    while (this.isRunning) {
      try {
        // Long polling automatically waits up to WaitTimeSeconds (20s)
        // If messages arrive, it returns immediately
        // If no messages after 20s, it returns empty array
        const messages = await this.sqsService.receiveMessages(10);

        if (messages.length > 0) {
          this.logger.debug(`📨 Received ${messages.length} messages [${this.workerId}]`);

          // Parse and accumulate messages
          const parsedMessages = messages.map(message => {
            try {
              const body = JSON.parse(message.Body);
              return {
                data: body.data,
                receiptHandle: message.ReceiptHandle,
                messageId: body.id,
              };
            } catch (error) {
              this.logger.error('❌ Error parsing message', error, {
                messageId: message.MessageId,
                workerId: this.workerId,
              });
              this.messagesFailed++;
              return null;
            }
          }).filter(msg => msg !== null) as Array<{data: any, receiptHandle: string, messageId: string}>;

          accumulatedMessages.push(...parsedMessages);
        }

        // Process batch if we've hit target size OR max wait time exceeded
        const waitTimeExceeded = (Date.now() - lastBatchTime) >= maxWaitTimeMs;
        const shouldProcessBatch = accumulatedMessages.length >= targetBatchSize || 
                                  (accumulatedMessages.length > 0 && waitTimeExceeded);

        if (shouldProcessBatch) {
          const batchSize = accumulatedMessages.length;
          this.logger.info(`🚀 Processing batch of ${batchSize} messages [${this.workerId}]`, {
            targetBatchSize,
            actualBatchSize: batchSize,
            waitTimeExceeded,
          });

          try {
            // 🛡️ AGGRESSIVE THROTTLING: Prevent hitting 500 statement limit
            const runningStatements = await this.redshiftService.getRunningStatementCount();
            
            if (runningStatements > 450) {
              // CRITICAL: Very close to limit - STOP processing
              this.logger.error(`🚨 CRITICAL: Redshift at ${runningStatements}/500 statements! [${this.workerId}]`, {
                runningStatements,
                limit: 500,
              });
              await this.sleep(30000); // Wait 30 seconds for Redshift to clear
              // Re-check after wait
              const recheck = await this.redshiftService.getRunningStatementCount();
              if (recheck > 400) {
                this.logger.warn(`Still high at ${recheck}, waiting another 30s [${this.workerId}]`);
                await this.sleep(30000);
              }
            } else if (runningStatements > 350) {
              // Approaching limit (500), slow down dramatically
              this.logger.warn(`⚠️  Redshift near capacity (${runningStatements}/500), throttling... [${this.workerId}]`, {
                runningStatements,
                limit: 500,
              });
              await this.sleep(20000); // Wait 20 seconds
            } else if (runningStatements > 250) {
              // High load, moderate throttling
              this.logger.info(`⚠️  Redshift high load (${runningStatements}/500), brief pause [${this.workerId}]`, {
                runningStatements,
              });
              await this.sleep(10000); // Wait 10 seconds
            } else if (runningStatements > 150) {
              // Medium load, light throttling
              this.logger.debug(`Redshift moderate load (${runningStatements}/500), small delay [${this.workerId}]`);
              await this.sleep(3000); // Wait 3 seconds
            }

            // Extract data for batch insert
            const dataArray = accumulatedMessages.map(msg => msg.data);
            
            // Batch insert into Redshift (single query for all rows!)
            await this.redshiftService.insertBatchData(dataArray);
            
            // All successful - batch delete all messages
            const receiptHandles = accumulatedMessages.map(msg => msg.receiptHandle);
            await this.sqsService.batchDeleteMessages(receiptHandles);
            
            this.messagesProcessed += batchSize;
            this.logger.info(`✅ Batch processed ${batchSize} messages [${this.workerId}]`);
            
            // Clear accumulated messages
            accumulatedMessages = [];
            lastBatchTime = Date.now();
            
            // ⚠️ MANDATORY RATE LIMITING: Always add delay to prevent overwhelming Redshift
            // Fire-and-forget mode means statements queue up fast, so we MUST slow down
            if (runningStatements > 300) {
              // High load - longer delay
              await this.sleep(15000); // 15 seconds
            } else if (runningStatements > 200) {
              // Moderate load - medium delay
              await this.sleep(10000); // 10 seconds
            } else if (runningStatements > 100) {
              // Light load - short delay
              await this.sleep(5000); // 5 seconds
            } else {
              // Low load - minimum delay to prevent queue buildup
              const batchDelay = parseInt(process.env.BATCH_DELAY_MS || '2000', 10);
              await this.sleep(batchDelay); // Default 2 seconds
            }
          } catch (error) {
            // Batch insert failed - messages will remain in queue and retry
            this.messagesFailed += batchSize;
            this.logger.error('❌ Batch processing failed', error, {
              workerId: this.workerId,
              messageCount: batchSize,
            });
            // Clear accumulated messages - they'll remain in SQS and be retried
            accumulatedMessages = [];
            lastBatchTime = Date.now();
            // Do NOT delete messages - they'll become visible again for retry
          }
        }

        // CRITICAL: NO sleep/delay here!
        // Long polling handles the wait internally (20s max)
        // Loop immediately continues to next poll
        // This maximizes throughput

      } catch (error) {
        this.logger.error('❌ Error polling SQS', error, {
          workerId: this.workerId,
        });
        // Only wait on errors to prevent tight error loops
        await this.sleep(5000);
      }
    }
  }


  /**
   * Log throughput metrics and Redshift statement health periodically
   */
  private startMetricsLogging(): void {
    const logInterval = setInterval(async () => {
      if (!this.isRunning) {
        clearInterval(logInterval);
        return;
      }

      const uptimeSeconds = (Date.now() - this.startTime) / 1000;
      const throughputPerSecond = this.messagesProcessed / uptimeSeconds;
      const throughputPerHour = throughputPerSecond * 3600;
      const throughputPerDay = throughputPerHour * 24;

      // Check running Redshift statement count
      const runningStatements = await this.redshiftService.getRunningStatementCount();

      this.logger.info(`📊 Throughput Stats [${this.workerId}]`, {
        workerId: this.workerId,
        messagesProcessed: this.messagesProcessed,
        messagesFailed: this.messagesFailed,
        uptimeSeconds: Math.floor(uptimeSeconds),
        throughputPerSecond: throughputPerSecond.toFixed(2),
        throughputPerHour: Math.floor(throughputPerHour),
        throughputPerDay: Math.floor(throughputPerDay),
        redshiftRunningStatements: runningStatements,
        redshiftStatementLimit: 500,
      });
    }, 60000); // Log every 60 seconds
  }

  /**
   * Sleep utility (only used for error backoff)
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
