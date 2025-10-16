import 'reflect-metadata';
import { Service } from 'typedi';
import { RedshiftDataClient } from '@aws-sdk/client-redshift-data';

export interface RedshiftConfig {
  clusterIdentifier: string;
  database: string;
  dbUser: string;
  tableName: string;
  region: string;
  statementTimeout: number;
  maxRetries: number;
}

@Service()
export class RedshiftClientFactory {
  private client: RedshiftDataClient;
  private config: RedshiftConfig;

  constructor() {
    // Initialize AWS Redshift Data API Client
    this.client = new RedshiftDataClient({
      region: process.env.AWS_REGION || 'us-east-1',
      credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
      },
    });

    // Load Redshift configuration from environment
    this.config = {
      clusterIdentifier: process.env.REDSHIFT_CLUSTER_IDENTIFIER || '',
      database: process.env.REDSHIFT_DATABASE || '',
      dbUser: process.env.REDSHIFT_DB_USER || '',
      tableName: process.env.REDSHIFT_TABLE_NAME || 'appsflyer_events',
      region: process.env.AWS_REGION || 'us-east-1',
      statementTimeout: parseInt(process.env.REDSHIFT_STATEMENT_TIMEOUT || '60000', 10), // 60 seconds
      maxRetries: parseInt(process.env.REDSHIFT_MAX_RETRIES || '30', 10), // 30 retries * 2s = 60s total
    };
  }

  getClient(): RedshiftDataClient {
    return this.client;
  }

  getConfig(): RedshiftConfig {
    return this.config;
  }

  getClusterIdentifier(): string {
    return this.config.clusterIdentifier;
  }

  getDatabase(): string {
    return this.config.database;
  }

  getDbUser(): string {
    return this.config.dbUser;
  }

  getTableName(): string {
    return this.config.tableName;
  }
}

