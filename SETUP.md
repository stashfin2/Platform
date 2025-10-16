# Platform Service - Setup Guide

## 🔐 Configuration Summary

### AWS Resources
- **Region:** `ap-south-1` (Mumbai)
- **SQS Queue:** `appflyer-queue`
- **Redshift Cluster:** `prod-upi-cluster`
- **Redshift Database:** `upi`

### Service Architecture
```
AppsFlyer Webhook → Fastify API → SQS Queue → Consumer → Redshift
```

## 📋 Environment Variables

All configuration is stored in `.env` file:

```env
# Server Configuration
PORT=3000
HOST=0.0.0.0
LOG_LEVEL=info

# SQS Consumer Configuration
ENABLE_SQS_CONSUMER=true
SQS_POLLING_INTERVAL=5000

# AWS Configuration
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=<your-key>
AWS_SECRET_ACCESS_KEY=<your-secret>

# AWS SQS Configuration
SQS_QUEUE_URL=https://sqs.ap-south-1.amazonaws.com/261962657740/appflyer-queue
SQS_MAX_MESSAGES=10
SQS_WAIT_TIME_SECONDS=20

# AWS Redshift Configuration
REDSHIFT_CLUSTER_IDENTIFIER=prod-upi-cluster
REDSHIFT_DATABASE=upi
REDSHIFT_DB_USER=awsuser
REDSHIFT_TABLE_NAME=appsflyer_events
REDSHIFT_STATEMENT_TIMEOUT=10000
REDSHIFT_MAX_RETRIES=10
```

## 🚀 Quick Start

### 1. Install Dependencies
```bash
npm install
```

### 2. Start Development Server
```bash
npm run dev
```

### 3. Start Production Server
```bash
npm run build
npm start
```

## 📡 API Endpoints

### Health Check
```bash
curl http://localhost:3000/health
```

### AppsFlyer Webhook
```bash
curl -X POST http://localhost:3000/api/webhook/appsflyer \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "af_purchase",
    "event_time": "2024-01-01 12:00:00",
    "app_id": "com.example.app",
    "customer_user_id": "user123",
    "idfa": "XXXX-XXXX-XXXX-XXXX"
  }'
```

### API Info
```bash
curl http://localhost:3000/api/info
```

## 🗄️ Redshift Table Schema

The service automatically creates this table:

```sql
CREATE TABLE IF NOT EXISTS appsflyer_events (
  id VARCHAR(255) PRIMARY KEY,
  event_time TIMESTAMP,
  event_name VARCHAR(255),
  app_id VARCHAR(255),
  user_id VARCHAR(255),
  device_id VARCHAR(255),
  event_data SUPER,
  created_at TIMESTAMP DEFAULT GETDATE()
);
```

## 🔄 Data Flow

1. **AppsFlyer** sends webhook to `/api/webhook/appsflyer`
2. **API** validates and pushes data to **SQS queue**
3. **SQS Consumer** polls queue every 5 seconds
4. **Consumer** processes messages and inserts into **Redshift**
5. Messages are deleted from queue after successful insertion

## 🛠️ Troubleshooting

### Check SQS Queue
```bash
aws sqs get-queue-attributes \
  --queue-url https://sqs.ap-south-1.amazonaws.com/261962657740/appflyer-queue \
  --attribute-names All
```

### Check Redshift Connection
```bash
aws redshift-data execute-statement \
  --cluster-identifier prod-upi-cluster \
  --database upi \
  --db-user awsuser \
  --sql "SELECT COUNT(*) FROM appsflyer_events;"
```

### Enable/Disable Consumer
Set in `.env`:
```env
ENABLE_SQS_CONSUMER=true   # Enable consumer
ENABLE_SQS_CONSUMER=false  # Disable consumer (API only)
```

## 📊 Monitoring

### View Logs
Development mode shows pretty-printed logs:
```
[2024-01-01 12:00:00] INFO: Message sent to SQS
    messageId: "abc-123"
    queueUrl: "https://sqs..."
```

### Production Logs (JSON)
```json
{
  "level": 30,
  "time": 1704110400000,
  "msg": "Message sent to SQS",
  "messageId": "abc-123"
}
```

## 🔒 Security Notes

- ✅ AWS credentials stored in `.env` (gitignored)
- ✅ Using Redshift Data API (no password in code)
- ✅ IAM-based authentication
- ✅ CORS enabled for webhook integration

## 📦 Deployment

### Docker
```bash
docker build -t platform-service .
docker run -p 3000:3000 --env-file .env platform-service
```

### AWS ECS/Fargate
1. Push to ECR
2. Create task definition with environment variables
3. Deploy as ECS service

## 🧪 Testing

### Test Webhook Locally
```bash
# Start service
npm run dev

# Send test webhook
curl -X POST http://localhost:3000/api/webhook/appsflyer \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "test_event",
    "event_time": "2024-01-01 12:00:00",
    "app_id": "test_app"
  }'
```

### Verify in Redshift
```sql
SELECT * FROM appsflyer_events 
ORDER BY created_at DESC 
LIMIT 10;
```

