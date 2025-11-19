import 'reflect-metadata';
import { S3Client } from '@aws-sdk/client-s3';
import { Service } from 'typedi';

export interface S3Config {
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  prefix: string; // Folder prefix for AppsFlyer data
}

@Service()
export class S3ClientFactory {
  private client: S3Client | null = null;
  private config: S3Config;

  constructor() {
    this.config = {
      region: process.env.AWS_REGION || 'us-east-1',
      bucket: process.env.S3_BUCKET || 'appsflyer-sqs',
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
      prefix: process.env.S3_PREFIX || 'appsflyer-events',
    };

    if (!this.config.accessKeyId || !this.config.secretAccessKey) {
      throw new Error('AWS credentials not configured. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY');
    }

    if (!this.config.bucket) {
      throw new Error('S3 bucket not configured. Set S3_BUCKET');
    }
  }

  getClient(): S3Client {
    if (!this.client) {
      this.client = new S3Client({
        region: this.config.region,
        credentials: {
          accessKeyId: this.config.accessKeyId,
          secretAccessKey: this.config.secretAccessKey,
        },
      });
    }
    return this.client;
  }

  getConfig(): S3Config {
    return this.config;
  }
}

