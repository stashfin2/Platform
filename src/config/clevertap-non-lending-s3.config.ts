import 'reflect-metadata';
import { S3Client } from '@aws-sdk/client-s3';
import { Service } from 'typedi';

export interface CleverTapNonLendingS3Config {
  region: string;
  bucket: string;
  accessKeyId: string;
  secretAccessKey: string;
}

/**
 * CleverTap Non-Lending Data S3 Client Factory
 * Handles the clevertap-non-lending-data bucket
 */
@Service()
export class CleverTapNonLendingS3ClientFactory {
  private client: S3Client | null = null;
  private config: CleverTapNonLendingS3Config;

  constructor() {
    this.config = {
      region: process.env.AWS_REGION || 'ap-south-1',
      bucket: process.env.CLEVERTAP_NON_LENDING_BUCKET || 'clevertap-non-lending-data',
      accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
    };

    if (!this.config.accessKeyId || !this.config.secretAccessKey) {
      throw new Error('AWS credentials not configured');
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

  getConfig(): CleverTapNonLendingS3Config {
    return this.config;
  }
}

