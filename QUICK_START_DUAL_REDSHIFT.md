# Quick Start: Dual Redshift Setup

## TL;DR

Your platform now supports writing data to a secondary Redshift instance in a different VPC via AWS PrivateLink. This is **opt-in** and doesn't affect existing functionality.

## Minimal Setup (5 Steps)

### 1. Set Up PrivateLink Endpoint (AWS Console)

1. Go to **VPC** → **Endpoints** → **Create Endpoint**
2. Service category: **AWS services**
3. Service: Search for `redshift-data` in your secondary region
4. Select your VPC and subnets
5. Enable DNS names
6. Select security group (allow HTTPS/443)
7. Copy the endpoint URL (looks like: `vpce-xxxxx.redshift-data.us-west-2.vpce.amazonaws.com`)

### 2. Create Table in Secondary Redshift

```bash
# Connect to your secondary Redshift and run
psql -h your-secondary-cluster.region.redshift.amazonaws.com -U admin -d analytics -f redshift-schema.sql
```

### 3. Add Environment Variables

Add these to your `.env` file:

```env
# Enable secondary Redshift
SECONDARY_REDSHIFT_ENABLED=true

# PrivateLink endpoint (ap-south-1 region)
SECONDARY_REDSHIFT_ENDPOINT=https://akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com

# Secondary cluster details
SECONDARY_AWS_REGION=ap-south-1
SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER=your-secondary-cluster
SECONDARY_REDSHIFT_DATABASE=analytics
SECONDARY_REDSHIFT_DB_USER=admin
SECONDARY_REDSHIFT_TABLE_NAME=appsflyer_events
```

### 4. Update IAM Policy

Add permission for secondary Redshift:

```json
{
  "Effect": "Allow",
  "Action": [
    "redshift-data:ExecuteStatement",
    "redshift-data:DescribeStatement"
  ],
  "Resource": "*"
}
```

### 5. Restart Your Service

```bash
npm run build
npm start
```

## Test It Works

### Send Test Event

```bash
curl -X POST http://localhost:3000/webhook/appsflyer \
  -H "Content-Type: application/json" \
  -d '{
    "appsflyer_id": "test-dual-123",
    "event_name": "test_event",
    "event_time": "2024-01-01 12:00:00",
    "app_id": "com.test.app",
    "platform": "ios"
  }'
```

### Check Logs

Look for:
```
✅ "Data successfully inserted into primary Redshift"
✅ "Data successfully inserted into secondary Redshift"
```

### Verify in Both Databases

**Primary:**
```sql
SELECT * FROM appsflyer_events WHERE appsflyer_id = 'test-dual-123';
```

**Secondary:**
```sql
SELECT * FROM appsflyer_events WHERE appsflyer_id = 'test-dual-123';
```

## How It Works

```
SQS Message → Service receives
              ↓
           Parse data
              ↓
     ┌────────┴────────┐
     │                 │
     ▼                 ▼
 Primary          Secondary
 Redshift         Redshift
     │                 │
     └────────┬────────┘
              ↓
      Both succeed? ✅
   Primary succeeds, secondary fails? ⚠️ (Still OK)
   Primary fails? ❌ (Message stays in queue for retry)
```

## Key Features

✅ **Parallel writes** - Both inserts happen simultaneously  
✅ **Fault tolerant** - Service continues if secondary fails  
✅ **Independent logging** - See results for each Redshift  
✅ **Easy to disable** - Set `SECONDARY_REDSHIFT_ENABLED=false`  
✅ **No code changes** - Pure configuration  

## Disable Anytime

```env
SECONDARY_REDSHIFT_ENABLED=false
```

Service continues working with primary Redshift only.

## Environment Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `SECONDARY_REDSHIFT_ENABLED` | Yes | `false` | Enable/disable dual-write |
| `SECONDARY_REDSHIFT_ENDPOINT` | No | - | PrivateLink endpoint (if cross-VPC) |
| `SECONDARY_AWS_REGION` | Yes* | - | AWS region of secondary cluster |
| `SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER` | Yes* | - | Cluster identifier |
| `SECONDARY_REDSHIFT_DATABASE` | Yes* | - | Database name |
| `SECONDARY_REDSHIFT_DB_USER` | Yes* | - | Database user |
| `SECONDARY_REDSHIFT_TABLE_NAME` | Yes* | `appsflyer_events` | Table name |
| `SECONDARY_REDSHIFT_STATEMENT_TIMEOUT` | No | `60000` | Timeout in ms |
| `SECONDARY_REDSHIFT_MAX_RETRIES` | No | `30` | Max retry attempts |

*Required when `SECONDARY_REDSHIFT_ENABLED=true`

## Troubleshooting

### Connection Timeout
```
Error: Timeout connecting to secondary Redshift
```
**Fix:** Verify VPC endpoint security group allows port 443

### Permission Denied
```
Error: User is not authorized
```
**Fix:** Check IAM policy includes `redshift-data:ExecuteStatement`

### Table Not Found
```
Error: relation "appsflyer_events" does not exist
```
**Fix:** Create table using `redshift-schema.sql` in secondary cluster

### Service Not Starting
```
Error: Secondary Redshift is not enabled
```
**Fix:** Check all required environment variables are set

## Need More Details?

- **Full Setup Guide**: `DUAL_REDSHIFT_SETUP.md`
- **Technical Changes**: `CHANGES_SUMMARY.md`
- **General Docs**: `README.md`

## Questions?

Common questions:

**Q: Will this slow down my API?**  
A: No. Both inserts run in parallel. Total time ≈ slowest insert time.

**Q: What if secondary Redshift is down?**  
A: Primary continues working. Secondary failures are logged but don't stop processing.

**Q: Can I use this without PrivateLink?**  
A: Yes! If secondary Redshift is in the same VPC or publicly accessible, just omit `SECONDARY_REDSHIFT_ENDPOINT`.

**Q: Do I need to change my application code?**  
A: No. This is pure configuration. No code changes needed.

**Q: Can I add more than 2 Redshift clusters?**  
A: Not currently. The system supports primary + one secondary. Contact dev team if you need more.

**Q: How much does this cost?**  
A: ~$10-50/month for PrivateLink + data transfer. Plus your secondary Redshift cluster costs.

---

**Status**: ✅ Ready to use  
**Build**: ✅ Passing  
**Tests**: Run `npm test`  
**Deploy**: Standard deployment process

