# Quick Start Guide

## ✅ Current Status

Your service is fully configured and ready to run! Here's what's been set up:

### Configured:
- ✅ SQS Queue: `appflyer-queue` 
- ✅ AWS Credentials: `appsflyer_user`
- ✅ Redshift Cluster: `prod-upi-cluster`
- ✅ Database: `upi`
- ✅ Region: `ap-south-1`

## 🚀 Setup Steps (One Time)

### Step 1: Create Redshift Table

Run this SQL script in your Redshift database:

```bash
# Using AWS CLI
aws redshift-data execute-statement \
  --cluster-identifier prod-upi-cluster \
  --database upi \
  --db-user awsuser \
  --sql file://redshift-schema.sql \
  --region ap-south-1
```

Or copy the contents of `redshift-schema.sql` and run it in Redshift Query Editor.

### Step 2: Grant IAM Permissions

Your user `appsflyer_user` needs Redshift Data API permissions. Follow the instructions in `REDSHIFT_SETUP.md` to attach the required IAM policy.

**Required Permissions:**
- `redshift-data:ExecuteStatement`
- `redshift-data:DescribeStatement`
- `sqs:SendMessage`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`

## 🎯 Start the Service

Once the table is created and permissions are granted:

```bash
# Install dependencies (if not already done)
npm install

# Start development server
npm run dev

# Or build and run production
npm run build
npm start
```

The service will:
1. Start API server on `http://localhost:3000`
2. Start SQS consumer (if `ENABLE_SQS_CONSUMER=true`)
3. Poll SQS queue every 5 seconds
4. Insert events into Redshift automatically

## 📡 Test the Setup

### 1. Check Health
```bash
curl http://localhost:3000/health
```

Expected response:
```json
{
  "status": "ok",
  "timestamp": "2024-01-01T00:00:00.000Z",
  "service": "Platform Service",
  "consumer": "enabled",
  "version": "1.0.0"
}
```

### 2. Send Test AppsFlyer Event
```bash
curl -X POST http://localhost:3000/api/webhook/appsflyer \
  -H "Content-Type: application/json" \
  -d '{
    "appsflyer_id": "test-1234567890",
    "event_name": "test_event",
    "event_time": "2024-01-01 12:00:00.000",
    "customer_user_id": "test_user@example.com",
    "platform": "ios",
    "app_id": "com.test.app",
    "media_source": "organic",
    "country_code": "IN",
    "event_source": "SDK"
  }'
```

Expected response:
```json
{
  "success": true,
  "messageId": "abc-123-def-456",
  "timestamp": "2024-01-01T00:00:00.000Z"
}
```

### 3. Verify in Redshift
```sql
-- Check if event was inserted
SELECT * FROM appsflyer_events 
WHERE appsflyer_id = 'test-1234567890';

-- Count all events
SELECT COUNT(*) FROM appsflyer_events;

-- Recent events
SELECT 
  appsflyer_id, 
  event_name, 
  event_time, 
  customer_user_id,
  platform,
  media_source
FROM appsflyer_events
ORDER BY created_at DESC
LIMIT 10;
```

## 🔄 Data Flow

```
┌─────────────┐       ┌─────────────┐       ┌─────────────┐       ┌─────────────┐
│  AppsFlyer  │──────▶│   Webhook   │──────▶│  SQS Queue  │──────▶│  Consumer   │
│             │ POST  │  /api/...   │ Push  │             │ Poll  │             │
└─────────────┘       └─────────────┘       └─────────────┘       └──────┬──────┘
                                                                           │
                                                                           │ Insert
                                                                           ▼
                                                                    ┌─────────────┐
                                                                    │  Redshift   │
                                                                    │    Table    │
                                                                    └─────────────┘
```

## 📊 AppsFlyer Webhook Configuration

In your AppsFlyer dashboard, configure the webhook:

**Webhook URL:** `https://your-domain.com/api/webhook/appsflyer`

**Events to Send:** Select all events you want to track
- Installs
- In-app events
- Re-engagements
- Purchases
- Custom events

**Format:** JSON (default)

## 🛠️ Configuration

All configuration is in `.env`:

```env
# Server
PORT=3000
ENABLE_SQS_CONSUMER=true  # Set to false to disable consumer

# AWS
AWS_REGION=ap-south-1
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...

# SQS
SQS_QUEUE_URL=https://sqs.ap-south-1.amazonaws.com/261962657740/appflyer-queue

# Redshift
REDSHIFT_CLUSTER_IDENTIFIER=prod-upi-cluster
REDSHIFT_DATABASE=upi
REDSHIFT_DB_USER=awsuser
REDSHIFT_TABLE_NAME=appsflyer_events
```

## 📁 Files Created

- ✅ `redshift-schema.sql` - Redshift table creation script
- ✅ `REDSHIFT_SETUP.md` - Detailed Redshift setup instructions
- ✅ `SETUP.md` - Complete service documentation
- ✅ `.env` - Your configuration (gitignored)

## 🐛 Troubleshooting

### Service won't start
- Check if port 3000 is available: `lsof -i:3000`
- Verify `.env` file exists and has correct values

### AccessDeniedException
- Grant IAM permissions (see `REDSHIFT_SETUP.md`)
- Verify AWS credentials are correct

### Data not appearing in Redshift
- Check SQS queue has messages: `aws sqs get-queue-attributes ...`
- Check consumer logs for errors
- Verify table exists: `SELECT * FROM appsflyer_events LIMIT 1;`

### Consumer keeps retrying messages
- Check Redshift connection
- Verify IAM permissions
- Check for duplicate `appsflyer_id` (primary key violation)

## 📚 More Documentation

- `SETUP.md` - Complete service documentation
- `REDSHIFT_SETUP.md` - Detailed Redshift setup & troubleshooting
- `README.md` - Project overview and architecture

## 🔐 Production Deployment

Before deploying to production:

1. ✅ Use AWS IAM roles instead of access keys (EC2/ECS/Fargate)
2. ✅ Set up VPC endpoints for Redshift and SQS
3. ✅ Enable HTTPS for webhook endpoint
4. ✅ Implement webhook signature verification (AppsFlyer provides)
5. ✅ Set up CloudWatch alarms for:
   - SQS queue depth
   - API error rate
   - Consumer processing time
6. ✅ Enable auto-scaling for consumer instances
7. ✅ Set up dead-letter queue for failed messages
8. ✅ Use AWS Secrets Manager for credentials

## 🚀 Next Steps

1. Run `redshift-schema.sql` to create the table
2. Grant IAM permissions
3. Start the service: `npm run dev`
4. Send a test webhook
5. Verify data in Redshift
6. Configure AppsFlyer to send webhooks to your service

You're all set! 🎉

