# Enable Secondary Redshift - Configuration Guide

## Your Specific Setup

You have a secondary Redshift accessible via AWS PrivateLink in the **ap-south-1 (Mumbai)** region.

**PrivateLink Endpoint:**
```
akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com
```

## Quick Enable (3 Steps)

### Step 1: Add These Variables to Your `.env` File

```env
# Enable secondary Redshift
SECONDARY_REDSHIFT_ENABLED=true

# PrivateLink endpoint (ap-south-1 Mumbai region)
SECONDARY_REDSHIFT_ENDPOINT=https://akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com

# Region
SECONDARY_AWS_REGION=ap-south-1

# Replace these with your actual secondary Redshift details:
SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER=your-cluster-name-here
SECONDARY_REDSHIFT_DATABASE=your-database-name-here
SECONDARY_REDSHIFT_DB_USER=your-db-user-here
SECONDARY_REDSHIFT_TABLE_NAME=appsflyer_events
```

### Step 2: Create Table in Secondary Redshift (ap-south-1)

Connect to your secondary Redshift cluster and run:

```bash
# If you have the schema file
psql -h your-secondary-cluster.ap-south-1.redshift.amazonaws.com \
     -U your_db_user \
     -d your_database \
     -f redshift-schema.sql

# Or manually run the CREATE TABLE statement from redshift-schema.sql
```

### Step 3: Restart Your Service

```bash
# Rebuild
npm run build

# Start
npm start

# Or for development
npm run dev
```

## Verify It's Working

### 1. Check Logs on Startup

You should see the service starting with secondary Redshift enabled:

```
[INFO] Starting platform service...
[INFO] Secondary Redshift enabled: true
```

### 2. Send a Test Event

```bash
curl -X POST http://localhost:3000/webhook/appsflyer \
  -H "Content-Type: application/json" \
  -d '{
    "appsflyer_id": "test-mumbai-123",
    "event_name": "test_dual_write_mumbai",
    "event_time": "2024-10-31 12:00:00",
    "app_id": "com.test.app",
    "customer_user_id": "test_user_123",
    "platform": "ios"
  }'
```

### 3. Check Application Logs

Look for these success messages:

```json
{
  "level": "info",
  "msg": "Data successfully inserted into primary Redshift",
  "statementId": "xxxxx",
  "target": "primary"
}
```

```json
{
  "level": "info",
  "msg": "Data successfully inserted into secondary Redshift",
  "statementId": "yyyyy",
  "target": "secondary"
}
```

### 4. Verify Data in Both Databases

**Primary Redshift:**
```sql
SELECT 
  appsflyer_id,
  event_name,
  event_time,
  platform
FROM appsflyer_events 
WHERE appsflyer_id = 'test-mumbai-123'
ORDER BY event_time DESC 
LIMIT 1;
```

**Secondary Redshift (ap-south-1):**
```sql
SELECT 
  appsflyer_id,
  event_name,
  event_time,
  platform
FROM appsflyer_events 
WHERE appsflyer_id = 'test-mumbai-123'
ORDER BY event_time DESC 
LIMIT 1;
```

Both should return the same record.

## IAM Permissions Required

Ensure your application's IAM role has these permissions for the ap-south-1 region:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "RedshiftDataAPIAccess",
      "Effect": "Allow",
      "Action": [
        "redshift-data:ExecuteStatement",
        "redshift-data:DescribeStatement",
        "redshift-data:GetStatementResult"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SecondaryRedshiftClusterAccess",
      "Effect": "Allow",
      "Action": [
        "redshift:GetClusterCredentials"
      ],
      "Resource": [
        "arn:aws:redshift:ap-south-1:YOUR_ACCOUNT_ID:cluster:YOUR_CLUSTER_NAME",
        "arn:aws:redshift:ap-south-1:YOUR_ACCOUNT_ID:dbuser:YOUR_CLUSTER_NAME/*",
        "arn:aws:redshift:ap-south-1:YOUR_ACCOUNT_ID:dbname:YOUR_CLUSTER_NAME/*"
      ]
    }
  ]
}
```

Replace:
- `YOUR_ACCOUNT_ID` with your AWS account ID
- `YOUR_CLUSTER_NAME` with your secondary cluster identifier

## Complete .env Example

Here's a complete example with both primary and secondary configured:

```env
# Server
PORT=3000
HOST=0.0.0.0
LOG_LEVEL=info

# SQS Consumer
ENABLE_SQS_CONSUMER=true
SQS_POLLING_INTERVAL=5000

# AWS
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=AKIAXXXXXXXXXX
AWS_SECRET_ACCESS_KEY=xxxxxxxxxxxxxxxxxxxxxx

# SQS
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/appsflyer-events

# PRIMARY REDSHIFT (e.g., us-east-1)
REDSHIFT_CLUSTER_IDENTIFIER=my-primary-cluster
REDSHIFT_DATABASE=analytics
REDSHIFT_DB_USER=admin
REDSHIFT_TABLE_NAME=appsflyer_events
REDSHIFT_STATEMENT_TIMEOUT=60000
REDSHIFT_MAX_RETRIES=30

# SECONDARY REDSHIFT (ap-south-1 Mumbai via PrivateLink)
SECONDARY_REDSHIFT_ENABLED=true
SECONDARY_REDSHIFT_ENDPOINT=https://akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com
SECONDARY_AWS_REGION=ap-south-1
SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER=my-mumbai-cluster
SECONDARY_REDSHIFT_DATABASE=analytics
SECONDARY_REDSHIFT_DB_USER=admin
SECONDARY_REDSHIFT_TABLE_NAME=appsflyer_events
SECONDARY_REDSHIFT_STATEMENT_TIMEOUT=60000
SECONDARY_REDSHIFT_MAX_RETRIES=30
```

## Troubleshooting

### Issue: "Cannot connect to secondary Redshift"

**Check:**
1. Endpoint URL is correct (starts with `https://`)
2. Security group allows outbound HTTPS (port 443)
3. VPC has route to PrivateLink endpoint
4. AWS credentials have permissions for ap-south-1 region

**Test connectivity:**
```bash
# From your application server
curl -v https://akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com
```

### Issue: "Permission denied"

**Solution:**
Update IAM policy to include ap-south-1 region resources (see IAM section above)

### Issue: "Table does not exist"

**Solution:**
Create the table in secondary Redshift using the same schema as primary:
```bash
psql -h YOUR_SECONDARY_CLUSTER.ap-south-1.redshift.amazonaws.com \
     -U admin -d analytics -f redshift-schema.sql
```

### Issue: "Secondary insert fails but primary works"

**This is expected behavior!** The system is designed to continue even if secondary fails.

**What happens:**
- ✅ Primary insert succeeds
- ❌ Secondary insert fails (logged as error)
- ✅ SQS message deleted (because primary succeeded)
- ⚠️ Data exists in primary but not secondary

**Check secondary logs and fix the issue**, then data will flow to both.

## Disable Secondary Redshift

To temporarily disable without removing configuration:

```env
SECONDARY_REDSHIFT_ENABLED=false
```

Service will continue with primary Redshift only.

## Monitor Dual-Write Performance

### CloudWatch Metrics to Track

1. **Success Rate per Target**
   - Primary insert success %
   - Secondary insert success %

2. **Latency**
   - Average time for primary inserts
   - Average time for secondary inserts

3. **Errors**
   - Count of secondary failures
   - Types of errors

### Sample CloudWatch Insights Query

```sql
fields @timestamp, msg, target, statementId, durationMs
| filter msg like /inserted into.*Redshift/
| stats count() by target, bin(5m)
```

## What's Next?

1. ✅ Configure environment variables
2. ✅ Create table in secondary Redshift
3. ✅ Restart service
4. ✅ Send test event
5. ✅ Verify data in both Redshifts
6. 📊 Set up monitoring/alerts
7. 🚀 Roll out to production

## Need Help?

- **Full Setup Guide**: See `DUAL_REDSHIFT_SETUP.md`
- **Quick Start**: See `QUICK_START_DUAL_REDSHIFT.md`
- **Architecture Details**: See `README.md`
- **Change Log**: See `CHANGES_SUMMARY.md`

---

**Your Configuration Summary:**
- 🏢 Primary: Your existing Redshift
- 🌏 Secondary: ap-south-1 (Mumbai) via PrivateLink
- 🔗 Endpoint: `akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com`
- ✅ Status: Ready to enable

