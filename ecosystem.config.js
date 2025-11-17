/**
 * PM2 Configuration for SQS Consumer Workers
 * 
 * Usage:
 *   pm2 start ecosystem.config.js --env production
 *   pm2 logs
 *   pm2 monit
 *   pm2 stop all
 * 
 * Scale workers:
 *   pm2 scale sqs-consumer 20
 */

module.exports = {
  apps: [
    {
      name: 'sqs-consumer',
      script: './dist/index.js',
      instances: 3, // Start with 3 workers for ra3.large cluster (prevents queue buildup)
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
      
      // Environment variables
      env: {
        NODE_ENV: 'development',
        PORT: 3000,
        HOST: '0.0.0.0',
        LOG_LEVEL: 'info',
        
        // SQS Configuration - OPTIMIZED
        ENABLE_SQS_CONSUMER: 'true',
        SQS_QUEUE_URL: 'https://sqs.ap-south-1.amazonaws.com/261962657740/appflyer-queue',
        SQS_MAX_MESSAGES: '10',
        SQS_WAIT_TIME_SECONDS: '20',
        
        // Rate Limiting - CRITICAL for small Redshift clusters
        BATCH_DELAY_MS: '2000', // 2 seconds between batches (prevents 500 statement limit)
        
        // AWS Credentials (use AWS_PROFILE or set these)
        AWS_REGION: 'ap-south-1',
        // AWS_ACCESS_KEY_ID: 'your_key',
        // AWS_SECRET_ACCESS_KEY: 'your_secret',
        
        // Redshift Configuration
        // REDSHIFT_CLUSTER_IDENTIFIER: 'your-cluster',
        // REDSHIFT_DATABASE: 'your_db',
        // REDSHIFT_DB_USER: 'your_user',
        // REDSHIFT_TABLE_NAME: 'appsflyer_events',
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

