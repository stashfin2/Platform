# Dual Redshift Setup Guide

This guide will help you configure the secondary Redshift instance for dual-write functionality across different VPCs using AWS PrivateLink.

## Overview

The platform now supports writing data to two Redshift clusters simultaneously:
- **Primary Redshift**: Your main data warehouse (required)
- **Secondary Redshift**: A replica in a different VPC (optional)

## Architecture

```
                    ┌─────────────────┐
                    │   SQS Consumer   │
                    └────────┬─────────┘
                             │
                ┌────────────┴────────────┐
                │                         │
                ▼                         ▼
        ┌───────────────┐         ┌───────────────┐
        │    Primary    │         │   Secondary   │
        │   Redshift    │         │   Redshift    │
        │   (VPC-A)     │         │   (VPC-B)     │
        └───────────────┘         └───────────────┘
                                          │
                                  via AWS PrivateLink
```

## Step 1: Set Up AWS PrivateLink (for Cross-VPC Communication)

### 1.1 Create VPC Endpoint in Source VPC

In the VPC where your application runs (where primary Redshift is located):

```bash
# Get your VPC ID
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=tag:Name,Values=your-vpc-name" --query 'Vpcs[0].VpcId' --output text)

# Get your subnet IDs (use private subnets)
SUBNET_IDS=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" --query 'Subnets[?MapPublicIpOnLaunch==`false`].SubnetId' --output text)

# Get your security group ID
SG_ID=$(aws ec2 describe-security-groups --filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=your-sg-name" --query 'SecurityGroups[0].GroupId' --output text)

# Create VPC endpoint for Redshift Data API
aws ec2 create-vpc-endpoint \
  --vpc-id $VPC_ID \
  --vpc-endpoint-type Interface \
  --service-name com.amazonaws.us-west-2.redshift-data \
  --subnet-ids $SUBNET_IDS \
  --security-group-ids $SG_ID \
  --region us-west-2
```

**Note**: Replace `us-west-2` with the region where your secondary Redshift is located.

### 1.2 Get the VPC Endpoint URL

```bash
# List VPC endpoints
aws ec2 describe-vpc-endpoints \
  --filters "Name=service-name,Values=com.amazonaws.us-west-2.redshift-data" \
  --query 'VpcEndpoints[0].DnsEntries[0].DnsName' \
  --output text
```

The output will be something like:
```
akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com
```

Save this for your environment configuration.

### 1.3 Update Security Group Rules

Ensure the security group allows HTTPS (port 443) traffic:

```bash
aws ec2 authorize-security-group-ingress \
  --group-id $SG_ID \
  --protocol tcp \
  --port 443 \
  --cidr 10.0.0.0/16  # Use your VPC CIDR block
```

## Step 2: Configure IAM Permissions

Your application's IAM role needs permissions for both Redshift clusters.

### IAM Policy Example

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
      "Sid": "PrimaryRedshiftAccess",
      "Effect": "Allow",
      "Action": [
        "redshift:GetClusterCredentials"
      ],
      "Resource": [
        "arn:aws:redshift:us-east-1:123456789012:cluster:primary-cluster",
        "arn:aws:redshift:us-east-1:123456789012:dbuser:primary-cluster/your_db_user",
        "arn:aws:redshift:us-east-1:123456789012:dbname:primary-cluster/your_database"
      ]
    },
    {
      "Sid": "SecondaryRedshiftAccess",
      "Effect": "Allow",
      "Action": [
        "redshift:GetClusterCredentials"
      ],
      "Resource": [
        "arn:aws:redshift:us-west-2:123456789012:cluster:secondary-cluster",
        "arn:aws:redshift:us-west-2:123456789012:dbuser:secondary-cluster/your_db_user",
        "arn:aws:redshift:us-west-2:123456789012:dbname:secondary-cluster/your_database"
      ]
    }
  ]
}
```

## Step 3: Create Table in Secondary Redshift

Connect to your secondary Redshift cluster and create the same table schema:

```sql
-- Use the same schema as primary
-- See redshift-schema.sql for the complete table definition

CREATE TABLE IF NOT EXISTS appsflyer_events (
  -- Copy the entire schema from your primary Redshift
  -- This ensures data compatibility
  ...
);
```

## Step 4: Configure Environment Variables

Update your `.env` file:

```env
# ============================================
# Primary Redshift Configuration
# ============================================
AWS_REGION=us-east-1
REDSHIFT_CLUSTER_IDENTIFIER=primary-cluster
REDSHIFT_DATABASE=analytics
REDSHIFT_DB_USER=admin
REDSHIFT_TABLE_NAME=appsflyer_events
REDSHIFT_STATEMENT_TIMEOUT=60000
REDSHIFT_MAX_RETRIES=30

# ============================================
# Secondary Redshift Configuration
# ============================================
SECONDARY_REDSHIFT_ENABLED=true

# Use the PrivateLink endpoint for ap-south-1 region
SECONDARY_REDSHIFT_ENDPOINT=https://akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com

# Region where secondary Redshift is located (Mumbai)
SECONDARY_AWS_REGION=ap-south-1

# Secondary cluster details
SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER=secondary-cluster
SECONDARY_REDSHIFT_DATABASE=analytics
SECONDARY_REDSHIFT_DB_USER=admin
SECONDARY_REDSHIFT_TABLE_NAME=appsflyer_events

# Optional: Override timeout and retry settings for secondary
SECONDARY_REDSHIFT_STATEMENT_TIMEOUT=60000
SECONDARY_REDSHIFT_MAX_RETRIES=30
```

## Step 5: Test the Configuration

### 5.1 Start the Service

```bash
npm run dev
```

### 5.2 Send a Test Event

```bash
curl -X POST http://localhost:3000/webhook/appsflyer \
  -H "Content-Type: application/json" \
  -d '{
    "appsflyer_id": "test-123",
    "event_name": "test_dual_write",
    "event_time": "2024-01-01 12:00:00",
    "app_id": "com.test.app",
    "customer_user_id": "test_user_123",
    "platform": "ios"
  }'
```

### 5.3 Check the Logs

Look for log entries indicating successful dual-write:

```json
{
  "level": "info",
  "msg": "Data successfully inserted into primary Redshift",
  "statementId": "abc-123",
  "target": "primary"
}
{
  "level": "info",
  "msg": "Data successfully inserted into secondary Redshift",
  "statementId": "def-456",
  "target": "secondary"
}
```

### 5.4 Verify Data in Both Redshift Clusters

**Primary Redshift:**
```sql
SELECT * FROM appsflyer_events 
WHERE appsflyer_id = 'test-123' 
ORDER BY event_time DESC 
LIMIT 1;
```

**Secondary Redshift:**
```sql
SELECT * FROM appsflyer_events 
WHERE appsflyer_id = 'test-123' 
ORDER BY event_time DESC 
LIMIT 1;
```

Both queries should return the same record.

## Step 6: Monitoring and Troubleshooting

### Monitor CloudWatch Logs

Key metrics to watch:
- Insertion success rate for primary vs. secondary
- Latency differences between clusters
- Error rates

### Common Issues

#### Issue 1: VPC Endpoint Connection Timeout

**Symptoms:**
```
Error: Connection timeout when connecting to secondary Redshift
```

**Solution:**
- Verify VPC endpoint is in the correct subnets
- Check security group allows port 443
- Ensure route tables are configured correctly

#### Issue 2: Permission Denied

**Symptoms:**
```
Error: User is not authorized to perform: redshift-data:ExecuteStatement
```

**Solution:**
- Review IAM policy (see Step 2)
- Ensure the application has the correct IAM role attached
- Verify cluster ARNs are correct

#### Issue 3: Secondary Insert Fails but Primary Succeeds

**Expected Behavior:** This is by design. The service will:
1. Log the error for secondary Redshift
2. Continue operating normally with primary Redshift
3. Delete the SQS message (since primary succeeded)

**Action:** 
- Check secondary Redshift logs
- Verify network connectivity
- Ensure table schema matches

### Health Check

The service continues to respond to health checks even if secondary Redshift is unavailable:

```bash
curl http://localhost:3000/health
```

Response:
```json
{
  "status": "ok",
  "timestamp": "2024-01-01T12:00:00.000Z",
  "consumer": "enabled"
}
```

## Step 7: Disable Secondary Redshift (Optional)

To temporarily disable dual-write without removing configuration:

```env
SECONDARY_REDSHIFT_ENABLED=false
```

The service will continue operating normally with only the primary Redshift.

## Performance Considerations

### Parallel Execution

Both inserts execute in parallel using `Promise.allSettled()`, so:
- Total time ≈ max(primary_time, secondary_time)
- Network latency to secondary cluster is the main factor

### Retry Logic

Each Redshift has independent retry logic:
- Default: 30 retries × 2 seconds = 60 seconds max wait
- Configurable via `REDSHIFT_MAX_RETRIES` and `SECONDARY_REDSHIFT_MAX_RETRIES`

### Failure Isolation

- Secondary failure doesn't impact primary operations
- SQS message is only deleted if primary succeeds
- Failed secondary inserts are logged for investigation

## Cost Implications

Dual-write increases costs:
1. **VPC Endpoint**: ~$0.01 per hour (~$7.30/month) per AZ
2. **Data Transfer**: Data transfer through PrivateLink (~$0.01 per GB)
3. **Redshift**: Storage and compute for secondary cluster
4. **Redshift Data API**: Charges per statement execution

Estimate ~$10-50/month extra depending on data volume.

## Rollback Plan

If you need to rollback:

1. Set `SECONDARY_REDSHIFT_ENABLED=false`
2. Restart the service
3. Delete the VPC endpoint (optional):
   ```bash
   aws ec2 delete-vpc-endpoints --vpc-endpoint-ids vpce-xxxxx
   ```

## Next Steps

- Set up CloudWatch alarms for insertion failures
- Configure data validation queries to ensure consistency
- Consider implementing periodic reconciliation jobs
- Set up monitoring dashboards for dual-write metrics

## Support

For issues or questions:
- Check application logs for detailed error messages
- Review AWS CloudWatch Logs
- Verify network connectivity between VPCs
- Ensure IAM permissions are correctly configured

