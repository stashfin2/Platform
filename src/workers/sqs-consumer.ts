import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { SQSService } from '../services/sqs.service';
import { RedshiftService } from '../services/redshift.service';
import { LoggerService } from '../services/logger.service';

@Service()
export class SQSConsumer {
  private isRunning: boolean = false;
  private pollingInterval: number;

  constructor(
    @Inject() private readonly sqsService: SQSService,
    @Inject() private readonly redshiftService: RedshiftService,
    @Inject() private readonly logger: LoggerService
  ) {
    this.pollingInterval = parseInt(process.env.SQS_POLLING_INTERVAL || '5000', 10);
  }

  /**
   * Start the SQS consumer
   */
  async start(): Promise<void> {
    this.isRunning = true;
    this.logger.info('SQS Consumer started. Polling for messages...');

    // Initialize Redshift table
    try {
      await this.redshiftService.createTableIfNotExists();
      this.logger.info('Redshift table initialized');
    } catch (error) {
      this.logger.error('Error initializing Redshift table', error);
    }

    // Start polling
    this.poll();
  }

  /**
   * Stop the SQS consumer
   */
  stop(): void {
    this.isRunning = false;
    this.logger.info('SQS Consumer stopped');
  }

  /**
   * Poll SQS for messages
   */
  private async poll(): Promise<void> {
    while (this.isRunning) {
      try {
        const messages = await this.sqsService.receiveMessages(10);

        if (messages.length > 0) {
          this.logger.info(`Received ${messages.length} messages from SQS`);

          // Process messages in parallel
          await Promise.all(
            messages.map(message => this.processMessage(message))
          );
        } else {
          // No messages, wait before next poll
          await this.sleep(this.pollingInterval);
        }
      } catch (error) {
        this.logger.error('Error polling SQS', error);
        // Wait before retrying
        await this.sleep(5000);
      }
    }
  }

  /**
   * Process a single SQS message
   */
  private async processMessage(message: any): Promise<void> {
    try {
      // Parse message body
      const body = JSON.parse(message.Body);
      this.logger.info('Processing message', {
        messageId: body.id,
        eventName: body.data?.event_name,
      });

      // Insert data into Redshift
      await this.redshiftService.insertData(body.data);
      this.logger.info('Data inserted into Redshift for message', {
        messageId: body.id,
      });

      // Delete message from SQS
      await this.sqsService.deleteMessage(message.ReceiptHandle);
      this.logger.info('Message deleted from SQS', {
        messageId: body.id,
      });
    } catch (error) {
      this.logger.error('Error processing message', error, {
        messageId: message.MessageId,
      });
      // Message will become visible again in SQS for retry
    }
  }

  /**
   * Sleep utility
   */
  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
