import 'reflect-metadata';
import { S3Client } from '@aws-sdk/client-s3';
import { Service } from 'typedi';

export interface CleverTapJsonS3Config {
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
}

/**
 * CleverTap JSON S3 Client Factory
 * Separate bucket for JSON storage before Redshift COPY
 */
@Service()
export class CleverTapJsonS3ClientFactory {
  private client: S3Client | null = null;
  private config: CleverTapJsonS3Config;

  constructor() {
    this.config = {
      region: process.env.AWS_REGION || 'ap-south-1',
      bucket: process.env.CLEVERTAP_JSON_BUCKET || 'clevertap-json',
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    };

    if (!this.config.accessKeyId || !this.config.secretAccessKey) {
      throw new Error('AWS credentials not configured');
    }

    console.log('🔧 CleverTap JSON S3 Configuration:', {
      region: this.config.region,
      bucket: this.config.bucket,
    });
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

  getConfig(): CleverTapJsonS3Config {
    return this.config;
  }
}



