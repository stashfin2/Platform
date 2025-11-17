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
   */
  private async poll(): Promise<void> {
    while (this.isRunning) {
      try {
        // Long polling automatically waits up to WaitTimeSeconds (20s)
        // If messages arrive, it returns immediately
        // If no messages after 20s, it returns empty array
        const messages = await this.sqsService.receiveMessages(10);

        if (messages.length > 0) {
          this.logger.info(`📨 Received ${messages.length} messages [${this.workerId}]`);

          // 🚀 BATCH PROCESSING: Process all messages together in a single Redshift INSERT
          try {
            // Parse all message bodies
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
                return null;
              }
            }).filter(msg => msg !== null);

            if (parsedMessages.length > 0) {
              // Extract data for batch insert
              const dataArray = parsedMessages.map(msg => msg!.data);
              
              // Batch insert into Redshift (single query for all rows!)
              await this.redshiftService.insertBatchData(dataArray);
              
              // All successful - batch delete all messages
              const receiptHandles = parsedMessages.map(msg => msg!.receiptHandle);
              await this.sqsService.batchDeleteMessages(receiptHandles);
              
              this.messagesProcessed += parsedMessages.length;
              this.logger.info(`✅ Batch processed ${parsedMessages.length} messages [${this.workerId}]`);
              
              // ⚠️ RATE LIMITING: Delay to prevent overwhelming Redshift
              // ra3.large can process ~10-15 statements/sec, so we throttle sending rate
              // This prevents the Redshift Data API queue from hitting 500 statement limit
              const batchDelay = parseInt('2000', 10);
              await this.sleep(batchDelay); // Default: 2 seconds between batches
            }

            // Track failed parsing
            const failedCount = messages.length - parsedMessages.length;
            if (failedCount > 0) {
              this.messagesFailed += failedCount;
              this.logger.warn(`⚠️  ${failedCount} messages failed parsing [${this.workerId}]`);
            }
          } catch (error) {
            // Batch insert failed - messages will remain in queue and retry
            this.messagesFailed += messages.length;
            this.logger.error('❌ Batch processing failed', error, {
              workerId: this.workerId,
              messageCount: messages.length,
            });
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
   * Log throughput metrics periodically
   */
  private startMetricsLogging(): void {
    const logInterval = setInterval(() => {
      if (!this.isRunning) {
        clearInterval(logInterval);
        return;
      }

      const uptimeSeconds = (Date.now() - this.startTime) / 1000;
      const throughputPerSecond = this.messagesProcessed / uptimeSeconds;
      const throughputPerHour = throughputPerSecond * 3600;
      const throughputPerDay = throughputPerHour * 24;

      this.logger.info(`📊 Throughput Stats [${this.workerId}]`, {
        workerId: this.workerId,
        messagesProcessed: this.messagesProcessed,
        messagesFailed: this.messagesFailed,
        uptimeSeconds: Math.floor(uptimeSeconds),
        throughputPerSecond: throughputPerSecond.toFixed(2),
        throughputPerHour: Math.floor(throughputPerHour),
        throughputPerDay: Math.floor(throughputPerDay),
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
