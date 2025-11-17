import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { 
  SendMessageCommand, 
  ReceiveMessageCommand, 
  DeleteMessageCommand,
  DeleteMessageBatchCommand,
  DeleteMessageBatchRequestEntry
} from '@aws-sdk/client-sqs';
import { v4 as uuidv4 } from 'uuid';
import { LoggerService } from './logger.service';
import { SQSClientFactory } from '../config/sqs.config';

@Service()
export class SQSService {
  constructor(
    @Inject() private readonly sqsClientFactory: SQSClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * Push data to SQS queue
   */
  async pushToQueue(data: any): Promise<string> {
    const client = this.sqsClientFactory.getClient();
    const queueUrl = this.sqsClientFactory.getQueueUrl();

    try {
      const messageId = uuidv4();
      const command = new SendMessageCommand({
        QueueUrl: queueUrl,
        MessageBody: JSON.stringify({
          id: messageId,
          timestamp: new Date().toISOString(),
          data,
        }),
        MessageAttributes: {
          Source: {
            DataType: 'String',
            StringValue: 'AppsFlyer',
          },
        },
      });

      const response = await client.send(command);
      this.logger.info('Message sent to SQS', {
        messageId: response.MessageId,
        queueUrl,
      });
      return response.MessageId || messageId;
    } catch (error) {
      this.logger.error('Error pushing to SQS', error, {
        queueUrl,
      });
      throw error;
    }
  }

  /**
   * Receive messages from SQS queue
   */
  async receiveMessages(maxMessages?: number): Promise<any[]> {
    const client = this.sqsClientFactory.getClient();
    const config = this.sqsClientFactory.getConfig();
    const queueUrl = this.sqsClientFactory.getQueueUrl();

    try {
      const command = new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: maxMessages || config.maxMessages,
        WaitTimeSeconds: config.waitTimeSeconds,
        MessageAttributeNames: ['All'],
      });

      const response = await client.send(command);
      return response.Messages || [];
    } catch (error) {
      this.logger.error('Error receiving from SQS', error, {
        queueUrl,
        maxMessages,
      });
      throw error;
    }
  }

  /**
   * Delete message from SQS queue after processing
   */
  async deleteMessage(receiptHandle: string): Promise<void> {
    const client = this.sqsClientFactory.getClient();
    const queueUrl = this.sqsClientFactory.getQueueUrl();

    try {
      const command = new DeleteMessageCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: receiptHandle,
      });

      await client.send(command);
      this.logger.debug('Message deleted from SQS', {
        queueUrl,
      });
    } catch (error) {
      this.logger.error('Error deleting message from SQS', error, {
        queueUrl,
      });
      throw error;
    }
  }

  /**
   * Batch delete messages from SQS queue
   * Much more efficient than individual deletes
   * SQS supports up to 10 messages per batch
   */
  async batchDeleteMessages(receiptHandles: string[]): Promise<void> {
    if (receiptHandles.length === 0) {
      return;
    }

    const client = this.sqsClientFactory.getClient();
    const queueUrl = this.sqsClientFactory.getQueueUrl();

    // Process in chunks of 10 (SQS batch limit)
    const chunks = this.chunkArray(receiptHandles, 10);

    try {
      await Promise.all(
        chunks.map(async (chunk) => {
          const entries: DeleteMessageBatchRequestEntry[] = chunk.map(
            (receiptHandle, index) => ({
              Id: `msg-${index}`,
              ReceiptHandle: receiptHandle,
            })
          );

          const command = new DeleteMessageBatchCommand({
            QueueUrl: queueUrl,
            Entries: entries,
          });

          const response = await client.send(command);

          if (response.Failed && response.Failed.length > 0) {
            this.logger.warn('Some messages failed to delete in batch', {
              failed: response.Failed.length,
              failedIds: response.Failed.map(f => f.Id),
            });
          }
        })
      );

      this.logger.debug(`Batch deleted ${receiptHandles.length} messages from SQS`, {
        queueUrl,
        totalMessages: receiptHandles.length,
      });
    } catch (error) {
      this.logger.error('Error batch deleting messages from SQS', error, {
        queueUrl,
        messageCount: receiptHandles.length,
      });
      throw error;
    }
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
