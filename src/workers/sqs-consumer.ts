import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { SQSService } from '../services/sqs.service';
import { RedshiftService } from '../services/redshift.service';
import { LoggerService } from '../services/logger.service';

interface MessageResult {
  message: any;
  success: boolean;
  receiptHandle: string;
}

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

          // Process messages in parallel with proper error handling
          const results = await Promise.allSettled(
            messages.map(message => this.processMessage(message))
          );

          // Collect successful message receipt handles for batch deletion
          const successfulReceipts: string[] = [];
          const failedReceipts: string[] = [];

          results.forEach((result, index) => {
            if (result.status === 'fulfilled' && result.value.success) {
              successfulReceipts.push(result.value.receiptHandle);
              this.messagesProcessed++;
            } else {
              failedReceipts.push(messages[index].ReceiptHandle);
              this.messagesFailed++;
            }
          });

          // Batch delete successful messages
          if (successfulReceipts.length > 0) {
            try {
              await this.sqsService.batchDeleteMessages(successfulReceipts);
              this.logger.info(`✅ Batch deleted ${successfulReceipts.length} messages [${this.workerId}]`);
            } catch (error) {
              this.logger.error('❌ Error batch deleting messages', error, {
                workerId: this.workerId,
                count: successfulReceipts.length,
              });
            }
          }

          if (failedReceipts.length > 0) {
            this.logger.warn(`⚠️  ${failedReceipts.length} messages failed processing [${this.workerId}]`);
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
   * Process a single SQS message
   * Returns MessageResult for batch deletion tracking
   */
  private async processMessage(message: any): Promise<MessageResult> {
    const receiptHandle = message.ReceiptHandle;
    
    try {
      // Parse message body
      const body = JSON.parse(message.Body);
      
      // Insert data into Redshift
      await this.redshiftService.insertData(body.data);
      
      return {
        message: body,
        success: true,
        receiptHandle,
      };
    } catch (error) {
      this.logger.error('❌ Error processing message', error, {
        messageId: message.MessageId,
        workerId: this.workerId,
      });
      
      // Return failure - message will NOT be deleted and will be retried
      return {
        message: null,
        success: false,
        receiptHandle,
      };
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
