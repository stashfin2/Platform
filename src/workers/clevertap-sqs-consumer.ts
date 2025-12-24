import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { CleverTapSQSService } from '../services/clevertap-sqs.service';
import { CleverTapRedshiftService, CleverTapEvent } from '../services/clevertap-redshift.service';
import { LoggerService } from '../services/logger.service';

/**
 * CleverTap SQS Consumer
 * Consumes messages from SQS containing CleverTap JSON rows
 * Batches inserts into Redshift for efficiency
 */
@Service()
export class CleverTapSqsConsumer {
  private isRunning: boolean = false;
  private workerId: string;
  private messagesProcessed: number = 0;
  private messagesFailed: number = 0;
  private rowsProcessed: number = 0;

  constructor(
    @Inject() private readonly sqsService: CleverTapSQSService,
    @Inject() private readonly redshiftService: CleverTapRedshiftService,
    @Inject() private readonly logger: LoggerService
  ) {
    this.workerId = `clevertap-consumer-${Date.now()}`;
  }

  /**
   * Start the CleverTap SQS consumer
   */
  async start(): Promise<void> {
    this.isRunning = true;
    this.logger.info('🚀 CleverTap SQS Consumer started', {
      workerId: this.workerId,
    });

    // Start polling (non-blocking - runs in background)
    this.poll();
  }

  /**
   * Stop the SQS consumer
   */
  stop(): void {
    this.isRunning = false;
    this.logger.info(`⛔ CleverTap SQS Consumer stopped [${this.workerId}]`, {
      workerId: this.workerId,
      messagesProcessed: this.messagesProcessed,
      messagesFailed: this.messagesFailed,
      rowsProcessed: this.rowsProcessed,
    });
  }

  /**
   * Poll SQS for messages continuously
   * Accumulates messages into batches and processes as soon as batch size is reached
   * Uses long polling (20s) but flushes immediately when batch is ready
   */
  private async poll(): Promise<void> {
    // Batch insert path (no COPY): default to small batches to keep SQL < 100KB and active statements low
    const targetBatchSize = parseInt(process.env.CLEVERTAP_REDSHIFT_BATCH_SIZE || '10', 10);
    // Flush quickly if not enough volume
    const maxWaitTimeMs = parseInt(process.env.MAX_BATCH_WAIT_MS || '1000', 10);
    
    let accumulatedEvents: CleverTapEvent[] = [];
    let receiptHandles: string[] = [];
    let lastBatchTime = Date.now();

    while (this.isRunning) {
      try {
        // Long polling automatically waits up to WaitTimeSeconds (20s)
        const messages = await this.sqsService.receiveMessages(10);

        if (messages.length > 0) {
          this.logger.debug(`📨 Received ${messages.length} messages [${this.workerId}]`);

          // Parse and extract events from messages
          const parsedMessages = messages.map(message => {
            try {
              const body = JSON.parse(message.Body);
              const messageData = body.data;
              
              // Extract rows from message (batched rows)
              if (messageData.rows && Array.isArray(messageData.rows)) {
                return {
                  events: messageData.rows as CleverTapEvent[],
                  receiptHandle: message.ReceiptHandle,
                  messageId: body.id,
                };
              }
              
              // Single row format (backward compatibility)
              return {
                events: [messageData as CleverTapEvent],
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
          }).filter(msg => msg !== null) as Array<{events: CleverTapEvent[], receiptHandle: string, messageId: string}>;

          // Accumulate events and receipt handles
          for (const parsed of parsedMessages) {
            accumulatedEvents.push(...parsed.events);
            receiptHandles.push(parsed.receiptHandle);
          }
        }

        const now = Date.now();
        const timeSinceLastBatch = now - lastBatchTime;
        
        // Flush immediately when batch size reached, or after maxWaitTimeMs if we have any data
        // This ensures fast processing: as soon as we have enough data OR after a short wait
        const shouldFlush = accumulatedEvents.length >= targetBatchSize || 
                           (accumulatedEvents.length > 0 && timeSinceLastBatch >= maxWaitTimeMs);

        if (shouldFlush && accumulatedEvents.length > 0) {
          this.logger.debug(`🔄 Flushing batch to Redshift`, {
            workerId: this.workerId,
            eventCount: accumulatedEvents.length,
            targetBatchSize,
            reason: accumulatedEvents.length >= targetBatchSize ? 'batch_size_reached' : 'timeout',
            waitTimeMs: timeSinceLastBatch,
          });
          const batchEvents = [...accumulatedEvents]; // Copy for this batch
          const batchReceiptHandles = [...receiptHandles]; // Copy for this batch
          
          try {
            // Insert batch into Redshift (waits for completion)
            const rowsInserted = await this.redshiftService.insertBatchData(batchEvents);
            
            // Only delete messages AFTER successful Redshift insert
            if (rowsInserted > 0) {
              this.rowsProcessed += rowsInserted;
              this.messagesProcessed += batchReceiptHandles.length;

              this.logger.info(`✅ Processed batch and inserted into Redshift`, {
                workerId: this.workerId,
                eventsProcessed: batchEvents.length,
                rowsInserted,
                messagesProcessed: batchReceiptHandles.length,
                totalRowsProcessed: this.rowsProcessed,
                totalMessagesProcessed: this.messagesProcessed,
              });

              // Delete messages from SQS ONLY after successful Redshift insert
              await this.sqsService.batchDeleteMessages(batchReceiptHandles);
              
              this.logger.info(`🗑️  Deleted ${batchReceiptHandles.length} messages from SQS after successful insert`, {
                workerId: this.workerId,
                messagesDeleted: batchReceiptHandles.length,
              });
            } else {
              // No rows inserted - might be an issue
              this.logger.warn('⚠️  No rows inserted into Redshift, not deleting messages', {
                workerId: this.workerId,
                eventCount: batchEvents.length,
              });
              this.messagesFailed += batchReceiptHandles.length;
            }

            // Reset accumulators
            accumulatedEvents = [];
            receiptHandles = [];
            lastBatchTime = Date.now();
          } catch (error) {
            this.logger.error('❌ Error processing batch - messages will NOT be deleted', error, {
              workerId: this.workerId,
              eventCount: batchEvents.length,
              messageCount: batchReceiptHandles.length,
              note: 'Messages will remain in SQS and retry automatically',
            });
            
            // Don't delete messages on error - let them retry
            // SQS will automatically redeliver after visibility timeout
            this.messagesFailed += batchReceiptHandles.length;
            
            // Reset accumulators to avoid reprocessing same events
            accumulatedEvents = [];
            receiptHandles = [];
            lastBatchTime = Date.now();
          }
        }
      } catch (error) {
        this.logger.error('❌ Error in poll loop', error, {
          workerId: this.workerId,
        });
        
        // Wait before retrying
        await new Promise(resolve => setTimeout(resolve, 5000));
      }
    }
  }
}

