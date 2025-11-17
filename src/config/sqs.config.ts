import 'reflect-metadata';
import { Service } from 'typedi';
import { SQSClient } from '@aws-sdk/client-sqs';

export interface SQSConfig {
  queueUrl: string;
  region: string;
  maxMessages: number;
  waitTimeSeconds: number;
}

/**
 * SQS Client Factory - Optimized for high-throughput message processing
 * 
 * Configuration Guidelines:
 * - maxMessages: Set to 10 (AWS maximum) for optimal batch processing
 * - waitTimeSeconds: Set to 20 (AWS maximum) for efficient long polling
 * - Long polling eliminates the need for artificial polling delays
 * - Use multiple worker instances to scale horizontally
 */
@Service()
export class SQSClientFactory {
  private client: SQSClient;
  private config: SQSConfig;

  constructor() {
    // Initialize AWS SQS Client with connection pooling
    this.client = new SQSClient({
      region: process.env.AWS_REGION || 'ap-south-1',
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
      },
      // Optimize for high throughput
      maxAttempts: 3,
      requestHandler: {
        // Connection pooling for better performance
        connectionTimeout: 5000,
        requestTimeout: 30000,
      },
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

    this.config = {
      queueUrl: process.env.SQS_QUEUE_URL || '',
      region: process.env.AWS_REGION || 'ap-south-1',
      maxMessages,
      waitTimeSeconds,
    };

    // Log configuration on startup
    console.log('🔧 SQS Configuration Loaded:', {
      region: this.config.region,
      maxMessages: this.config.maxMessages,
      waitTimeSeconds: this.config.waitTimeSeconds,
      queueUrl: this.config.queueUrl ? '✅ Set' : '❌ Missing',
    });
  }

  getClient(): SQSClient {
    return this.client;
  }

  getConfig(): SQSConfig {
    return this.config;
  }

  getQueueUrl(): string {
    return this.config.queueUrl;
  }
}

