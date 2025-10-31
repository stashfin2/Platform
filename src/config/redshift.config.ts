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

/**
 * Secondary Redshift Client Factory for different VPC (via PrivateLink)
 * This allows dual-write to a secondary Redshift instance
 */
@Service()
export class SecondaryRedshiftClientFactory {
  private client: RedshiftDataClient;
  private config: RedshiftConfig;
  private enabled: boolean;

  constructor() {
    // Check if secondary Redshift is enabled
    this.enabled = process.env.SECONDARY_REDSHIFT_ENABLED === 'true';

    if (this.enabled) {
      // Initialize AWS Redshift Data API Client for secondary instance
      // Use endpoint URL for PrivateLink connection if provided
      const clientConfig: any = {
        region: process.env.SECONDARY_AWS_REGION || process.env.AWS_REGION || 'us-east-1',
        credentials: {
          accessKeyId: process.env.AWS_ACCESS_KEY_ID || '',
          secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || '',
        },
      };

      // Add custom endpoint for PrivateLink if configured
      if (process.env.SECONDARY_REDSHIFT_ENDPOINT) {
        clientConfig.endpoint = process.env.SECONDARY_REDSHIFT_ENDPOINT;
      }

      this.client = new RedshiftDataClient(clientConfig);

      // Load secondary Redshift configuration from environment
      this.config = {
        clusterIdentifier: process.env.SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER || '',
        database: process.env.SECONDARY_REDSHIFT_DATABASE || '',
        dbUser: process.env.SECONDARY_REDSHIFT_DB_USER || '',
        tableName: process.env.SECONDARY_REDSHIFT_TABLE_NAME || 'appsflyer.appsflyer_events',
        region: process.env.SECONDARY_AWS_REGION || process.env.AWS_REGION || 'ap-souht-1',
        statementTimeout: parseInt(process.env.SECONDARY_REDSHIFT_STATEMENT_TIMEOUT || '60000', 10),
        maxRetries: parseInt(process.env.SECONDARY_REDSHIFT_MAX_RETRIES || '30', 10),
      };
    } else {
      // Initialize with empty values when disabled
      this.client = null as any;
      this.config = {} as RedshiftConfig;
    }
  }

  isEnabled(): boolean {
    return this.enabled;
  }

  getClient(): RedshiftDataClient {
    if (!this.enabled) {
      throw new Error('Secondary Redshift is not enabled');
    }
    return this.client;
  }

  getConfig(): RedshiftConfig {
    if (!this.enabled) {
      throw new Error('Secondary Redshift is not enabled');
    }
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

