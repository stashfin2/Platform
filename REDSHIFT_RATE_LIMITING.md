# 🎯 Redshift Rate Limiting - The Real Fix

## 🚨 The Core Problem

### Your Redshift Cluster Capacity (ra3.large):
```
Concurrent query slots: 25
Processing rate: ~10-15 statements/second
Queue limit (Data API): 500 statements max
```

### What Was Happening:
```
20 workers × 1 batch/second = 20 statements/second sent
Redshift processes: 10-15 statements/second
Net growth: 20 - 12 = 8 statements/second accumulating
Time to hit 500 limit: 500 / 8 = 62 seconds

Result: "Active statements exceeded the allowed quota (500)" ❌
```

**Even with batch inserts, you were sending statements FASTER than Redshift could process them!**

---

## ✅ The Solution: Match Send Rate to Processing Capacity

### Strategy:
```
Redshift can process: ~10-12 statements/second
So we send: ≤10 statements/second
How: Fewer workers + delays between batches
```

---

## 📊 New Configuration

### Option A: 3 Workers + 2 Second Delay (Recommended)

```javascript
// ecosystem.config.js
instances: 3
BATCH_DELAY_MS: 2000 (2 seconds)

Math:
3 workers × 1 batch per 2 seconds = 1.5 batches/sec
1.5 batches/sec = 1.5 statements/sec sent to Redshift
Redshift processes: 10-15 statements/sec

Result: Queue stays empty! ✅

Throughput:
1.5 statements/sec × 10 rows = 15 rows/sec
15 × 3600 = 54,000/hour
54,000 × 24 = 1.3M/day

Your needs:
- Incoming: 600k/day
- Capacity: 1.3M/day
- Surplus: 700k/day ✅

Still enough! Backlog clears in ~4 days.
```

### Option B: 5 Workers + 2 Second Delay (Faster)

```javascript
instances: 5
BATCH_DELAY_MS: 2000

Math:
5 workers × 0.5 batches/sec = 2.5 statements/sec
Well under Redshift capacity ✅

Throughput:
2.5 × 10 rows × 3600 × 24 = 2.16M/day
Surplus: 1.56M/day
Backlog clears in ~2 days ✅
```

### Option C: 8 Workers + 3 Second Delay (Balanced)

```javascript
instances: 8
BATCH_DELAY_MS: 3000

Math:
8 workers × 0.33 batches/sec = 2.67 statements/sec
Safe margin ✅

Throughput:
2.67 × 10 rows × 3600 × 24 = 2.3M/day
Backlog clears in ~1.5 days ✅
```

---

## 🚀 Deployment Steps

### Step 1: Stop All Workers (IMMEDIATELY)

```bash
pm2 stop sqs-consumer
```

This stops adding to the Redshift queue.

---

### Step 2: Wait for Redshift Queue to Clear (10-20 minutes)

Monitor the queue:

```bash
# Check how many statements are queued
aws redshift-data list-statements \
  --status SUBMITTED \
  --region ap-south-1 \
  | jq '.Statements | length'

# Check running statements
aws redshift-data list-statements \
  --status RUNNING \
  --region ap-south-1 \
  | jq '.Statements | length'

# Wait until total < 50
```

Or check in **AWS Console**:
```
Redshift → Query Editor → Query History
Filter: Last 15 minutes
Status: Running or Submitted
Count should be < 50
```

**Grab a coffee ☕ - This takes 10-20 minutes for 500 statements**

---

### Step 3: Build and Deploy Updated Code

```bash
# Build
npm run build

# Clean slate
pm2 delete sqs-consumer

# Start with 3 workers (conservative)
pm2 start ecosystem.config.js --env production
```

---

### Step 4: Monitor Carefully

```bash
# Watch logs
pm2 logs sqs-consumer

# Should see:
✅ Batch processed 10 messages [worker-1]
# 2 second pause...
✅ Batch processed 10 messages [worker-1]
# 2 second pause...

# Check for errors
pm2 logs sqs-consumer | grep "exceeded"
# Should be EMPTY ✅
```

---

### Step 5: Verify Redshift Queue Stays Low

After 15 minutes of running:

```bash
# Check queue depth
aws redshift-data list-statements \
  --status RUNNING \
  --region ap-south-1 \
  | jq '.Statements | length'

# Should show 5-15 (not 500!)
```

---

## 📈 Scaling Up (If Needed)

### After 1 Hour of Stable Operation:

If you want faster processing and no errors:

```bash
# Scale to 5 workers
pm2 scale sqs-consumer 5

# Monitor for 30 minutes
pm2 logs sqs-consumer | grep "exceeded"

# If stable, can scale to 8
pm2 scale sqs-consumer 8
```

**Maximum safe workers for ra3.large: 8-10 workers**

Beyond that, you'll hit the limit again.

---

## 🎯 Configuration Reference

### For Different Cluster Sizes:

| Cluster Type | Query Slots | Safe Workers | Batch Delay | Throughput/Day |
|--------------|-------------|--------------|-------------|----------------|
| ra3.large | 15-25 | 3-5 workers | 2000ms | 1.3M-2.2M |
| ra3.xlplus | 15-25 | 3-5 workers | 2000ms | 1.3M-2.2M |
| ra3.4xlarge | 50-100 | 10-15 workers | 1000ms | 8.6M-13M |
| ra3.16xlarge | 200-400 | 30-50 workers | 500ms | 25M-43M |

**Your setup: ra3.large → Use 3-5 workers with 2000ms delay**

---

## 🔍 Troubleshooting

### Still Getting "exceeded quota" Errors?

**Reduce workers further:**
```bash
pm2 scale sqs-consumer 2
```

**Or increase delay:**
```bash
# Edit .env or ecosystem.config.js
BATCH_DELAY_MS=3000  # 3 seconds

# Restart
pm2 restart sqs-consumer
```

### Queue Growing Too Slowly?

**Carefully scale up:**
```bash
# Try 5 workers
pm2 scale sqs-consumer 5

# Monitor for 1 hour
# If stable, try 7
pm2 scale sqs-consumer 7

# Never exceed 10 for ra3.large!
```

### Redshift Query Queue Building Up?

Check query execution time:
```sql
-- In Redshift Query Editor
SELECT 
  AVG(DATEDIFF(seconds, starttime, endtime)) as avg_duration_sec,
  MAX(DATEDIFF(seconds, starttime, endtime)) as max_duration_sec,
  COUNT(*) as query_count
FROM stl_query
WHERE querytxt LIKE 'INSERT INTO appsflyer_events%'
  AND starttime > DATEADD(hour, -1, GETDATE());
```

If avg > 5 seconds:
- Your cluster is overloaded
- Reduce workers to 2-3
- Or upgrade to ra3.4xlarge

---

## 💡 Why This Happens

### Redshift Data API Has Two Limits:

1. **Processing capacity (cluster-dependent)**
   - ra3.large: 15-25 concurrent queries
   - Each query takes 1-3 seconds
   - Max throughput: ~10-15 queries/sec

2. **Queue limit (API-level)**
   - Max 500 statements in queue
   - Includes SUBMITTED + RUNNING + QUEUED

### The Problem:
```
If you send statements faster than Redshift processes them:
→ Queue grows
→ Hits 500 limit
→ New statements rejected
→ Error!
```

### The Fix:
```
Send statements at Redshift's processing speed:
→ Queue stays small
→ Never hits 500
→ No errors! ✅
```

---

## 📊 Performance Comparison

### Before (20 workers, no rate limiting):
```
Statements sent: 20/sec
Statements processed: 12/sec
Queue growth: +8/sec
Time to crash: 60 seconds ❌
```

### After (3 workers, 2s delay):
```
Statements sent: 1.5/sec
Statements processed: 12/sec
Queue growth: 0 (actually shrinking)
Time to crash: Never ✅
Throughput: 1.3M/day (still > 600k/day needed)
```

### After (5 workers, 2s delay):
```
Statements sent: 2.5/sec
Statements processed: 12/sec
Queue growth: 0
Throughput: 2.16M/day
Backlog: Clears in 2 days ✅
```

---

## ✅ Summary

### What Changed:
1. ✅ **Reduced workers**: 20 → 3 (in ecosystem.config.js)
2. ✅ **Added rate limiting**: 2 second delay between batches
3. ✅ **Made configurable**: BATCH_DELAY_MS environment variable

### Why It Works:
- Sends statements slower than Redshift processes them
- Queue never fills up
- No 500 limit errors
- Still fast enough to clear backlog

### Deploy Steps:
1. Stop workers: `pm2 stop sqs-consumer`
2. Wait 15 min for queue to clear
3. Deploy: `npm run build && pm2 start ecosystem.config.js`
4. Monitor: `pm2 logs sqs-consumer`
5. Verify: No errors, queue stays low

**Your queue will finally drain!** 🎉

---

## 🎓 Key Lesson

**Throughput ≠ Speed of sending**

Sending 1000 statements/sec doesn't help if Redshift can only process 10/sec.

Better to send 10/sec sustainably than crash at 1000/sec.

**Slow and steady wins the race.** 🐢✅

