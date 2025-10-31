import 'reflect-metadata';
import { Service } from 'typedi';
import { SQSClient } from '@aws-sdk/client-sqs';

export interface SQSConfig {
  queueUrl: string;
  region: string;
  maxMessages: number;
  waitTimeSeconds: number;
}

@Service()
export class SQSClientFactory {
  private client: SQSClient;
  private config: SQSConfig;

  constructor() {
    // Initialize AWS SQS Client
    this.client = new SQSClient({
      region: process.env.AWS_REGION || 'ap-south-1',
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
      },
    });

    // Load SQS configuration from environment
    this.config = {
      queueUrl: process.env.SQS_QUEUE_URL || '',
      region: process.env.AWS_REGION || 'ap-south-1',
      maxMessages: parseInt(process.env.SQS_MAX_MESSAGES || '10', 10),
      waitTimeSeconds: parseInt(process.env.SQS_WAIT_TIME_SECONDS || '20', 10),
    };
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

