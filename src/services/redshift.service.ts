import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { ExecuteStatementCommand, DescribeStatementCommand } from '@aws-sdk/client-redshift-data';
import { LoggerService } from './logger.service';
import { RedshiftClientFactory } from '../config/redshift.config';

@Service()
export class RedshiftService {
  constructor(
    @Inject() private readonly redshiftClientFactory: RedshiftClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * Insert data into Redshift table
   */
  async insertData(data: any): Promise<string> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();

    try {
      // Parse the data if it's a string
      const parsedData = typeof data === 'string' ? JSON.parse(data) : data;
      
      // Extract fields from the data (adjust based on AppsFlyer schema)
      const values = this.prepareInsertValues(parsedData);
      
      // Create INSERT statement
      const sql = `
        INSERT INTO ${config.tableName} 
        (id, event_time, event_name, app_id, user_id, device_id, event_data, created_at)
        VALUES (
          '${values.id}',
          '${values.eventTime}',
          '${values.eventName}',
          '${values.appId}',
          '${values.userId}',
          '${values.deviceId}',
          '${JSON.stringify(values.eventData).replace(/'/g, "''")}',
          GETDATE()
        );
      `;

      const command = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: sql,
      });

      const response = await client.send(command);
      const statementId = response.Id || '';
      
      this.logger.info('Data inserted into Redshift', {
        statementId,
        tableName: config.tableName,
        eventName: values.eventName,
      });
      
      // Wait for statement to complete
      await this.waitForStatementCompletion(statementId);
      
      return statementId;
    } catch (error) {
      this.logger.error('Error inserting data into Redshift', error, {
        tableName: config.tableName,
      });
      throw error;
    }
  }

  /**
   * Prepare values for INSERT statement from AppsFlyer data
   */
  private prepareInsertValues(data: any): any {
    return {
      id: data.id || data.event_id || '',
      eventTime: data.timestamp || data.event_time || new Date().toISOString(),
      eventName: data.event_name || data.eventName || 'unknown',
      appId: data.app_id || data.appId || '',
      userId: data.customer_user_id || data.userId || '',
      deviceId: data.idfa || data.advertising_id || data.deviceId || '',
      eventData: data,
    };
  }

  /**
   * Wait for Redshift statement to complete
   */
  private async waitForStatementCompletion(statementId: string): Promise<void> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();

    for (let i = 0; i < config.maxRetries; i++) {
      const command = new DescribeStatementCommand({ Id: statementId });
      const response = await client.send(command);
      
      const status = response.Status;
      
      if (status === 'FINISHED') {
        this.logger.debug('Redshift statement completed successfully', {
          statementId,
        });
        return;
      } else if (status === 'FAILED' || status === 'ABORTED') {
        const errorMessage = response.Error || 'Unknown error';
        this.logger.error('Redshift statement failed', new Error(errorMessage), {
          statementId,
          status,
        });
        throw new Error(`Redshift statement failed: ${errorMessage}`);
      }
      
      // Wait before next check
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    
    this.logger.error('Timeout waiting for Redshift statement to complete', undefined, {
      statementId,
      maxRetries: config.maxRetries,
    });
    throw new Error('Timeout waiting for Redshift statement to complete');
  }

  /**
   * Create table if not exists (for initial setup)
   */
  async createTableIfNotExists(): Promise<void> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();

    const sql = `
      CREATE TABLE IF NOT EXISTS ${config.tableName} (
        id VARCHAR(255) PRIMARY KEY,
        event_time TIMESTAMP,
        event_name VARCHAR(255),
        app_id VARCHAR(255),
        user_id VARCHAR(255),
        device_id VARCHAR(255),
        event_data SUPER,
        created_at TIMESTAMP DEFAULT GETDATE()
      );
    `;

    try {
      const command = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: sql,
      });

      const response = await client.send(command);
      this.logger.info('Table creation statement executed', {
        statementId: response.Id,
        tableName: config.tableName,
      });
      
      if (response.Id) {
        await this.waitForStatementCompletion(response.Id);
      }
    } catch (error) {
      this.logger.error('Error creating table', error, {
        tableName: config.tableName,
      });
      throw error;
    }
  }
}
