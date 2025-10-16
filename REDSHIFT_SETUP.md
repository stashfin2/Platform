# Redshift Setup Guide

## 📋 Prerequisites

Before running the service, you need to:
1. Create the Redshift table manually
2. Grant proper IAM permissions to your AWS user

## 1️⃣ Create Redshift Table

### Option A: Using Redshift Query Editor

1. Go to AWS Console → Amazon Redshift → Query Editor V2
2. Connect to your cluster: `prod-upi-cluster`
3. Select database: `upi`
4. Run the SQL script from `redshift-schema.sql`

### Option B: Using psql CLI

```bash
psql -h prod-upi-cluster.cmtdfhmjao7g.ap-south-1.redshift.amazonaws.com \
     -p 5439 \
     -U awsuser \
     -d upi \
     -f redshift-schema.sql
```

### Option C: Using AWS CLI

```bash
aws redshift-data execute-statement \
  --cluster-identifier prod-upi-cluster \
  --database upi \
  --db-user awsuser \
  --sql file://redshift-schema.sql
```

## 2️⃣ Grant IAM Permissions

Your IAM user `appsflyer_user` needs the following permissions:

### Create IAM Policy

Create a file named `redshift-data-policy.json`:

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
        "redshift-data:GetStatementResult",
        "redshift-data:CancelStatement",
        "redshift-data:ListStatements"
      ],
      "Resource": "arn:aws:redshift:ap-south-1:261962657740:cluster:prod-upi-cluster"
    },
    {
      "Sid": "RedshiftGetCredentials",
      "Effect": "Allow",
      "Action": [
        "redshift:GetClusterCredentials",
        "redshift:DescribeClusters"
      ],
      "Resource": [
        "arn:aws:redshift:ap-south-1:261962657740:cluster:prod-upi-cluster",
        "arn:aws:redshift:ap-south-1:261962657740:dbuser:prod-upi-cluster/awsuser",
        "arn:aws:redshift:ap-south-1:261962657740:dbname:prod-upi-cluster/upi"
      ]
    },
    {
      "Sid": "SQSAccess",
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:ap-south-1:261962657740:appflyer-queue"
    }
  ]
}
```

### Attach Policy to User

```bash
# Create the policy
aws iam create-policy \
  --policy-name AppsFlyerRedshiftDataAPIPolicy \
  --policy-document file://redshift-data-policy.json

# Attach to user
aws iam attach-user-policy \
  --user-name appsflyer_user \
  --policy-arn arn:aws:iam::261962657740:policy/AppsFlyerRedshiftDataAPIPolicy
```

## 3️⃣ Verify Setup

### Test Redshift Connection

```bash
aws redshift-data execute-statement \
  --cluster-identifier prod-upi-cluster \
  --database upi \
  --db-user awsuser \
  --sql "SELECT COUNT(*) FROM appsflyer_events;"
```

### Test SQS Access

```bash
aws sqs get-queue-attributes \
  --queue-url https://sqs.ap-south-1.amazonaws.com/261962657740/appflyer-queue \
  --attribute-names All
```

## 📊 Table Schema Overview

The `appsflyer_events` table includes:

### Primary Identifiers
- `appsflyer_id` (PRIMARY KEY)
- `event_name`
- `event_time`
- `customer_user_id`

### User Identifiers (9 fields)
- IDFA, IDFV, Advertising ID, Android ID, OAID, Amazon AID, IMEI

### App Information (7 fields)
- App ID, Name, Version, Bundle ID, Platform, SDK Version, API Version

### Device Information (6 fields)
- Device Type, Model, Category, OS Version, Language, User Agent

### Location Information (7 fields)
- Country Code, Region, State, City, Postal Code, DMA, IP

### Attribution Information (50+ fields)
- Media Source, Campaign, Keywords, Ads, Channels, Sub-parameters
- Contributors (1, 2, 3) for multi-touch attribution
- Install and touch times
- Match types and engagement types

### Event Information (10 fields)
- Event Type, Source, Value, Revenue (USD & local currency)

### Privacy & Consent (5 fields)
- LAT, GDPR, Ad Personalization, Consent Data

### Optimization Features
- **SORTKEY**: `(event_time, appsflyer_id)` - Fast time-based queries
- **DISTKEY**: `customer_user_id` - Distributed by user for user-level analytics
- **Indexes**: On common query columns (event_time, event_name, customer_user_id, etc.)

## 4️⃣ Common Queries

### View Recent Events
```sql
SELECT 
  appsflyer_id,
  event_name,
  event_time,
  customer_user_id,
  platform,
  media_source,
  campaign
FROM appsflyer_events
ORDER BY event_time DESC
LIMIT 100;
```

### Count Events by Name
```sql
SELECT 
  event_name,
  COUNT(*) as event_count
FROM appsflyer_events
WHERE event_time >= DATEADD(day, -7, GETDATE())
GROUP BY event_name
ORDER BY event_count DESC;
```

### Attribution Analysis
```sql
SELECT 
  media_source,
  campaign,
  COUNT(*) as installs,
  COUNT(DISTINCT customer_user_id) as unique_users
FROM appsflyer_events
WHERE event_name = 'install'
  AND event_time >= DATEADD(day, -30, GETDATE())
GROUP BY media_source, campaign
ORDER BY installs DESC;
```

### Revenue by Platform
```sql
SELECT 
  platform,
  COUNT(*) as purchase_count,
  SUM(event_revenue_usd) as total_revenue_usd
FROM appsflyer_events
WHERE event_name IN ('af_purchase', 'purchase')
  AND event_revenue_usd IS NOT NULL
  AND event_time >= DATEADD(day, -7, GETDATE())
GROUP BY platform;
```

## 🔍 Troubleshooting

### Issue: AccessDeniedException
**Error:** `User is not authorized to perform: redshift-data:ExecuteStatement`

**Solution:** Attach the IAM policy (Step 2) to your user.

### Issue: Table does not exist
**Error:** `relation "appsflyer_events" does not exist`

**Solution:** Run the CREATE TABLE script (Step 1).

### Issue: Primary key violation
**Error:** `duplicate key value violates unique constraint`

**Solution:** AppsFlyer is sending duplicate `appsflyer_id`. You can either:
1. Use UPSERT logic (UPDATE if exists, INSERT if not)
2. Add a unique constraint on `(appsflyer_id, event_time)`

To enable UPSERT, modify the code to use:
```sql
BEGIN TRANSACTION;
DELETE FROM appsflyer_events WHERE appsflyer_id = 'xxx';
INSERT INTO appsflyer_events (...) VALUES (...);
END TRANSACTION;
```

## 📈 Performance Tips

1. **Vacuuming**: Run `VACUUM` regularly to reclaim space
```sql
VACUUM appsflyer_events;
```

2. **Analyzing**: Update statistics for better query planning
```sql
ANALYZE appsflyer_events;
```

3. **Partitioning**: For large datasets, consider partitioning by month
```sql
CREATE TABLE appsflyer_events_202401 (LIKE appsflyer_events);
-- Insert data for January 2024
```

4. **Compression**: Redshift automatically compresses columns, but you can optimize:
```sql
ANALYZE COMPRESSION appsflyer_events;
```

## 🔐 Security Best Practices

1. ✅ Use IAM roles instead of access keys when running on AWS (EC2, ECS, Lambda)
2. ✅ Rotate access keys regularly
3. ✅ Use VPC endpoints for Redshift and SQS
4. ✅ Enable encryption at rest for Redshift
5. ✅ Enable CloudTrail logging for API calls
6. ✅ Use AWS Secrets Manager for credentials in production

