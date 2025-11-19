/**
 * PM2 Configuration for SQS Consumer Workers
 * 
 * Usage:
 *   pm2 start ecosystem.config.js --env production
 *   pm2 logs
 *   pm2 monit
 *   pm2 stop all
 * 
 * Scale workers dynamically:
 *   pm2 scale sqs-consumer 20   # Increase to 20 workers
 *   pm2 scale sqs-consumer 5    # Decrease to 5 workers
 */

module.exports = {
  apps: [
    {
      name: 'sqs-consumer',
      script: './dist/index.js',
      instances: 5, // Balanced: 5 workers with aggressive throttling = 650k-850k/day
      exec_mode: 'cluster',
      
      // Auto-restart configuration
      autorestart: true,
      watch: false,
      max_memory_restart: '1G',
      
      // Logging
      error_file: './logs/error.log',
      out_file: './logs/out.log',
      log_date_format: 'YYYY-MM-DD HH:mm:ss Z',
      merge_logs: true,
      
      // Environment variables (PRODUCTION CONFIGURATION)
      // All AWS credentials and Redshift config come from .env file
      env: {
        NODE_ENV: 'production',
        PORT: 6000,
        HOST: '0.0.0.0',
        LOG_LEVEL: 'info',
        
        // SQS Configuration - OPTIMIZED
        ENABLE_SQS_CONSUMER: 'true',
        SQS_MAX_MESSAGES: '10', // AWS SQS maximum
        SQS_WAIT_TIME_SECONDS: '20', // Long polling
        
        // Batch Size Configuration (CRITICAL!)
        // NOTE: Redshift Data API has 100KB query size limit
        // AppsFlyer schema is VERY large (100+ columns + JSON fields)
        // Even 50 rows exceeds limit! Using 20 for safety
        TARGET_BATCH_SIZE: '20', // Conservative batch size to stay well under 100KB limit
        MAX_BATCH_WAIT_MS: '3000', // Max 3 seconds wait (shorter for smaller batches)
        
        // Rate Limiting - MANDATORY delay to prevent overwhelming Redshift
        // With fire-and-forget mode, we MUST add delays between batches
        BATCH_DELAY_MS: '2000', // 2 second minimum delay between batches
        
        // Disable secondary Redshift (only use primary)
        ENABLE_SECONDARY_REDSHIFT: 'false',
        
        // NOTE: All credentials and endpoints come from .env file:
        // - AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
        // - SQS_QUEUE_URL
        // - REDSHIFT_CLUSTER_IDENTIFIER, REDSHIFT_DATABASE, REDSHIFT_DB_USER, REDSHIFT_TABLE_NAME
      },
      
      env_production: {
        NODE_ENV: 'production',
        LOG_LEVEL: 'info',
      },
      
      // Kill timeout
      kill_timeout: 5000,
      
      // Wait time before restart
      restart_delay: 4000,
      
      // Max restarts within time window
      max_restarts: 10,
      min_uptime: '10s',
    },
  ],
};

