/**
 * PM2 Configuration for SQS Consumer Workers
 * 
 * THROUGHPUT TARGET: 700,000 messages/day (8.1 msgs/sec sustained)
 * 
 * How this achieves 700k/day:
 *   - 10 workers × 10 messages/batch × 1 batch/sec = 100 messages/sec theoretical
 *   - With adaptive throttling: ~8-10 messages/sec sustained = 700k-850k/day
 *   - Redshift fire-and-forget mode allows high throughput
 *   - Adaptive throttling prevents hitting 500 statement limit
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
      instances: 10, // 10 workers = ~100 msgs/sec theoretical, ~8-10 msgs/sec with throttling
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
        SQS_MAX_MESSAGES: '10', // AWS SQS maximum
        SQS_WAIT_TIME_SECONDS: '20', // Long polling
        
        // Batch Size Configuration (NEW!)
        TARGET_BATCH_SIZE: '100', // Accumulate 100 messages per INSERT (10x more efficient!)
        MAX_BATCH_WAIT_MS: '5000', // Max 5 seconds wait to accumulate messages
        
        // Rate Limiting - Adaptive throttling based on Redshift load
        // Set to 0 for maximum throughput, adaptive throttling kicks in when needed
        BATCH_DELAY_MS: '0', // No base delay, relies on adaptive throttling (300+ statements = slow down)
        
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

