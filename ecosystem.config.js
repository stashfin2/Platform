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
      name: 'api-server',
      script: './dist/index.js',
      instances: 1, // Single API server instance
      exec_mode: 'cluster',
      
      // Auto-restart configuration
      autorestart: true,
      watch: false,
      max_memory_restart: '4G',
      
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
        
        // S3 Configuration (Optimized for 800k msgs/day)
        S3_BUCKET: 'appsflyer-sqs',
        S3_PREFIX: 'appsflyer-events',
        S3_BATCH_SIZE: '500', // 500 events per file (optimized for high volume)
        S3_BATCH_FLUSH_INTERVAL: '30000', // 30 seconds
        
        // S3 Sync Configuration
        ENABLE_S3_SYNC: 'true',
        S3_SYNC_INTERVAL_MS: '3600000', // 1 hour (3600000ms)
        
        // Disable secondary Redshift (only use primary)
        ENABLE_SECONDARY_REDSHIFT: 'false',
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

