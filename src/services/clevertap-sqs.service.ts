import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { 
  SendMessageBatchCommand,
  ReceiveMessageCommand, 
  DeleteMessageBatchCommand,
  DeleteMessageBatchRequestEntry,
  SendMessageBatchRequestEntry
} from '@aws-sdk/client-sqs';
import { v4 as uuidv4 } from 'uuid';
import { LoggerService } from './logger.service';
import { CleverTapSQSClientFactory } from '../config/clevertap-sqs.config';

/**
 * CleverTap SQS Service
 * Dedicated SQS service for CleverTap queue
 */
@Service()
export class CleverTapSQSService {
  constructor(
    @Inject() private readonly sqsClientFactory: CleverTapSQSClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * Batch push multiple messages to CleverTap SQS queue
   * More efficient than individual sends (up to 10 messages per batch)
   */
  async batchPushToQueue(messages: Array<{ data: any; messageAttributes?: Record<string, any> }>): Promise<string[]> {
    const client = this.sqsClientFactory.getClient();
    const queueUrl = this.sqsClientFactory.getQueueUrl();

    if (messages.length === 0) {
      return [];
    }

    // SQS batch limit is 10 messages per batch
    const chunks = this.chunkArray(messages, 10);
    const messageIds: string[] = [];

    try {
      await Promise.all(
        chunks.map(async (chunk) => {
          const entries: SendMessageBatchRequestEntry[] = chunk.map((msg, index) => {
            const messageId = uuidv4();
            const messageBody = JSON.stringify({
              id: messageId,
              timestamp: new Date().toISOString(),
              data: msg.data,
            });

            return {
              Id: `msg-${index}`,
              MessageBody: messageBody,
              MessageAttributes: {
                Source: {
                  DataType: 'String',
                  StringValue: msg.messageAttributes?.Source || 'CleverTap',
                },
                ...msg.messageAttributes,
              },
            };
          });

          const command = new SendMessageBatchCommand({
            QueueUrl: queueUrl,
            Entries: entries,
          });

          const response = await client.send(command);
          
          if (response.Successful) {
            messageIds.push(...(response.Successful.map(s => s.MessageId || '')));
          }

          if (response.Failed && response.Failed.length > 0) {
            this.logger.warn('Some messages failed to send in batch', {
              failed: response.Failed.length,
              failedIds: response.Failed.map(f => f.Id),
            });
          }
        })
      );

      this.logger.info(`Batch sent ${messageIds.length} messages to CleverTap SQS`, {
        queueUrl,
        totalMessages: messages.length,
      });

      return messageIds;
    } catch (error) {
      this.logger.error('Error batch pushing to CleverTap SQS', error, {
        queueUrl,
        messageCount: messages.length,
      });
      throw error;
    }
  }

  /**
   * Receive messages from CleverTap SQS queue
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
      this.logger.error('Error receiving from CleverTap SQS', error, {
        queueUrl,
        maxMessages,
      });
      throw error;
    }
  }

  /**
   * Batch delete messages from CleverTap SQS queue
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

      this.logger.debug(`Batch deleted ${receiptHandles.length} messages from CleverTap SQS`, {
        queueUrl,
        totalMessages: receiptHandles.length,
      });
    } catch (error) {
      this.logger.error('Error batch deleting messages from CleverTap SQS', error, {
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

