# Fix Your .env Configuration

## Current Issues

Based on your error and .env file:

### Issue 1: Primary Redshift Can't Connect
```
Error: Redshift endpoint doesn't exist in this region.
Cluster: prod-upi-cluster
Region: ap-south-1
```

### Issue 2: Secondary Table Name is Wrong
```
SECONDARY_REDSHIFT_TABLE_NAME=Ap@43$pfl&5@y(r&789
```
This should be the actual table name like `appsflyer_events` or `appsflyer.appsflyer_events`

## Quick Fix Steps

### Step 1: Verify Your Primary Cluster Exists

Run this command to list your Redshift clusters in ap-south-1:

```bash
aws redshift describe-clusters \
  --region ap-south-1 \
  --query 'Clusters[*].[ClusterIdentifier,ClusterStatus,Endpoint.Address]' \
  --output table
```

**Check:**
- Is `prod-upi-cluster` in the list?
- Is it in `available` status?
- What's the endpoint address?

### Step 2: Fix Your .env File

Based on the output above, update your `.env`:

```env
# If primary cluster needs PrivateLink too (if in same VPC as secondary)
AWS_REGION=ap-south-1
REDSHIFT_CLUSTER_IDENTIFIER=prod-upi-cluster
REDSHIFT_DATABASE=upi
REDSHIFT_DB_USER=awsuser
REDSHIFT_TABLE_NAME=appsflyer.appsflyer_events
REDSHIFT_STATEMENT_TIMEOUT=10000
REDSHIFT_MAX_RETRIES=10

# Secondary Redshift
SECONDARY_REDSHIFT_ENABLED=true
SECONDARY_REDSHIFT_ENDPOINT=https://akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com
SECONDARY_AWS_REGION=ap-south-1
SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER=analytics-cluster
SECONDARY_REDSHIFT_DATABASE=sttash-db
SECONDARY_REDSHIFT_DB_USER=appsflyer
SECONDARY_REDSHIFT_TABLE_NAME=appsflyer_events  # <-- FIX THIS!
```

### Step 3: Fix the Secondary Table Name

The table name should match the table in your secondary database. Most likely:

```env
SECONDARY_REDSHIFT_TABLE_NAME=appsflyer_events
# OR if it's in a schema:
SECONDARY_REDSHIFT_TABLE_NAME=appsflyer.appsflyer_events
```

## Possible Scenarios

### Scenario A: Primary Needs PrivateLink Too

If BOTH clusters are accessed via PrivateLink, you need to update the code to support endpoint for primary too.

### Scenario B: Primary is Public, Secondary is PrivateLink

This should work, but verify:
1. Your primary cluster is publicly accessible OR in the same VPC as your app
2. Security groups allow access

### Scenario C: Wrong Cluster Identifier

Maybe the cluster name is different. Check with:

```bash
aws redshift describe-clusters --region ap-south-1 --query 'Clusters[*].ClusterIdentifier'
```

## Test Each Cluster Separately

### Test 1: Disable Secondary (test primary only)

```env
SECONDARY_REDSHIFT_ENABLED=false
```

Then restart and test. If this works, primary is fine.

### Test 2: Check if Primary Also Needs PrivateLink

If both clusters require PrivateLink, we need to add endpoint support for primary.

## Next Steps

1. Run the AWS CLI command to verify cluster names
2. Share the output with me
3. Fix the table name
4. Let me know if primary also needs PrivateLink support

---

## Quick Commands

```bash
# List clusters in ap-south-1
aws redshift describe-clusters --region ap-south-1

# Test if you can reach primary cluster
aws redshift-data execute-statement \
  --cluster-identifier prod-upi-cluster \
  --database upi \
  --db-user awsuser \
  --sql "SELECT 1;" \
  --region ap-south-1

# Test if you can reach secondary cluster via PrivateLink
aws redshift-data execute-statement \
  --cluster-identifier analytics-cluster \
  --database sttash-db \
  --db-user appsflyer \
  --sql "SELECT 1;" \
  --region ap-south-1 \
  --endpoint-url https://akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com
```

