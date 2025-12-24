import 'reflect-metadata';
import { Service } from 'typedi';
import { SQSClient } from '@aws-sdk/client-sqs';
import { NodeHttpHandler } from '@smithy/node-http-handler';

export interface CleverTapSQSConfig {
  queueUrl: string;
  region: string;
  maxMessages: number;
  waitTimeSeconds: number;
}

/**
 * CleverTap SQS Client Factory
 * Separate SQS configuration for CleverTap queue
 */
@Service()
export class CleverTapSQSClientFactory {
  private client: SQSClient;
  private config: CleverTapSQSConfig;

  constructor() {
    // Initialize AWS SQS Client with connection pooling
    const requestTimeoutMs = parseInt(process.env.CLEVERTAP_SQS_REQUEST_TIMEOUT_MS || '20000', 10);
    const connectionTimeoutMs = parseInt(process.env.CLEVERTAP_SQS_CONNECTION_TIMEOUT_MS || '5000', 10);

    this.client = new SQSClient({
      region: process.env.AWS_REGION || 'ap-south-1',
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
      },
      // Optimize for high throughput and allow more time for slow networks
      maxAttempts: 3,
      requestHandler: new NodeHttpHandler({
        connectionTimeout: connectionTimeoutMs,
        socketTimeout: requestTimeoutMs,
      }),
    });

    // Load and validate SQS configuration from environment
    const maxMessages = parseInt(process.env.SQS_MAX_MESSAGES || '10', 10);
    const waitTimeSeconds = parseInt(process.env.SQS_WAIT_TIME_SECONDS || '20', 10);

    // Validate configuration
    if (maxMessages < 1 || maxMessages > 10) {
      throw new Error('SQS_MAX_MESSAGES must be between 1 and 10 (AWS limit)');
    }
    if (waitTimeSeconds < 0 || waitTimeSeconds > 20) {
      throw new Error('SQS_WAIT_TIME_SECONDS must be between 0 and 20 (AWS limit)');
    }

    // Use CleverTap-specific queue URL if provided, otherwise fall back to general SQS_QUEUE_URL
    const queueUrl = process.env.CLEVERTAP_SQS_QUEUE_URL || process.env.SQS_QUEUE_URL || '';

    this.config = {
      queueUrl,
      region: process.env.AWS_REGION || 'ap-south-1',
      maxMessages,
      waitTimeSeconds,
    };

    // Log configuration on startup
    console.log('🔧 CleverTap SQS Configuration Loaded:', {
      region: this.config.region,
      maxMessages: this.config.maxMessages,
      waitTimeSeconds: this.config.waitTimeSeconds,
      queueUrl: this.config.queueUrl ? '✅ Set' : '❌ Missing',
      requestTimeoutMs,
      connectionTimeoutMs,
    });
  }

  getClient(): SQSClient {
    return this.client;
  }

  getConfig(): CleverTapSQSConfig {
    return this.config;
  }

  getQueueUrl(): string {
    return this.config.queueUrl;
  }
}

