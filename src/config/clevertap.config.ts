import 'reflect-metadata';
import { S3Client } from '@aws-sdk/client-s3';
import { Service } from 'typedi';

export interface CleverTapS3Config {
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
  prefix: string; // Folder prefix for CleverTap data (usually empty for root)
}

@Service()
export class CleverTapS3ClientFactory {
  private client: S3Client | null = null;
  private config: CleverTapS3Config;

  constructor() {
    this.config = {
      region: process.env.AWS_REGION || 'ap-south-1',
      bucket: process.env.CLEVERTAP_S3_BUCKET || 'clevertap-essential-data',
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
      prefix: process.env.CLEVERTAP_S3_PREFIX || '', // Root folder by default
    };

    if (!this.config.accessKeyId || !this.config.secretAccessKey) {
      throw new Error('AWS credentials not configured. Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY');
    }

    if (!this.config.bucket) {
      throw new Error('CleverTap S3 bucket not configured. Set CLEVERTAP_S3_BUCKET');
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

  getConfig(): CleverTapS3Config {
    return this.config;
  }
}

