# 🚀 SQS Consumer Scaling Guide

## ⚠️ CRITICAL: Your System is Under-Provisioned

### Current Situation

- **Backlog**: 2,800,000 messages (28 lakh)
- **Daily Incoming**: 600,000 messages/day (6 lakh)
- **Required Processing**: 800,000 - 1,000,000 messages/day to clear backlog
- **Current Capacity**: ~172,800 messages/day per worker (MAX theoretical)

**Reality**: You need **5-10 workers minimum** running 24×7 to survive.

---

## 📊 The Math: Why You're Failing

### Single Worker Capacity (Before Optimization)

```
Old Config:
- Poll every 5 seconds → 12 polls/min → 720 polls/hour
- 10 messages per poll
- 720 × 10 = 7,200 messages/hour
- 7,200 × 24 = 172,800 messages/day

With 600k incoming/day:
- Net backlog growth: +427,200 messages/day 💀
- Your queue will NEVER drain
```

### Single Worker Capacity (After Optimization)

```
New Config:
- Long polling (20s wait) eliminates delays
- Continuous polling → ~3 polls/min → 180 polls/hour
- 10 messages per poll
- 180 × 10 = 1,800 messages/hour
- 1,800 × 24 = 43,200 messages/day

Still terrible, but 2-3x better than before.
```

### What You Actually Need

```
To process 800k-1M messages/day:
- 800,000 ÷ 43,200 = 18.5 workers minimum
- Realistically: 20-25 workers for buffer

To clear backlog in 2 weeks:
- Backlog: 2.8M
- Incoming: 600k/day × 14 = 8.4M
- Total to process: 11.2M messages in 14 days
- Required: 800k/day
- Workers needed: 18-20

To clear backlog in 1 week:
- Total: 11.2M in 7 days = 1.6M/day
- Workers needed: 37+
```

**Conservative Recommendation: Start with 15-20 workers**

---

## 🛠️ Implementation: How to Scale

### Option 1: Manual - Run Multiple Processes Locally (Quick Test)

```bash
# Terminal 1
WORKER_ID=worker-1 npm start

# Terminal 2
WORKER_ID=worker-2 npm start

# Terminal 3
WORKER_ID=worker-3 npm start

# ... repeat for N workers
```

**Use Case**: Quick testing, development  
**Pros**: Simple, immediate  
**Cons**: Not production-ready, manual management

---

### Option 2: PM2 - Process Manager (Simple Production)

Install PM2:
```bash
npm install -g pm2
```

Create `ecosystem.config.js`:
```javascript
module.exports = {
  apps: [
    {
      name: 'sqs-consumer',
      script: 'dist/index.js',
      instances: 15, // Number of workers
      exec_mode: 'cluster',
      env: {
        NODE_ENV: 'production',
        PORT: 3000,
        ENABLE_SQS_CONSUMER: 'true',
        // Add all your other env vars here
      },
      env_production: {
        NODE_ENV: 'production'
      }
    }
  ]
};
```

Run:
```bash
# Start 15 workers
pm2 start ecosystem.config.js --env production

# Monitor
pm2 monit

# Logs
pm2 logs sqs-consumer

# Stop
pm2 stop sqs-consumer

# Restart
pm2 restart sqs-consumer
```

**Use Case**: Single EC2 instance, VPS  
**Pros**: Easy, built-in monitoring, auto-restart  
**Cons**: Single point of failure

---

### Option 3: Docker + Docker Compose (Multi-Container)

Create `Dockerfile`:
```dockerfile
FROM node:18-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY . .
RUN npm run build

CMD ["node", "dist/index.js"]
```

Create `docker-compose.yml`:
```yaml
version: '3.8'

services:
  sqs-consumer:
    build: .
    environment:
      - ENABLE_SQS_CONSUMER=true
      - AWS_REGION=ap-south-1
      - SQS_QUEUE_URL=${SQS_QUEUE_URL}
      - SQS_MAX_MESSAGES=10
      - SQS_WAIT_TIME_SECONDS=20
      - AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
      - AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
      - REDSHIFT_CLUSTER_IDENTIFIER=${REDSHIFT_CLUSTER_IDENTIFIER}
      - REDSHIFT_DATABASE=${REDSHIFT_DATABASE}
      - REDSHIFT_DB_USER=${REDSHIFT_DB_USER}
      - REDSHIFT_TABLE_NAME=${REDSHIFT_TABLE_NAME}
      # Add all other required env vars
    deploy:
      replicas: 15  # Number of workers
      restart_policy:
        condition: on-failure
        delay: 5s
        max_attempts: 3
    mem_limit: 512m
    cpus: 0.5
```

Run:
```bash
# Start 15 workers
docker-compose up -d --scale sqs-consumer=15

# View logs
docker-compose logs -f sqs-consumer

# Stop
docker-compose down
```

**Use Case**: Containerized deployments  
**Pros**: Reproducible, portable, resource limits  
**Cons**: Requires Docker infrastructure

---

### Option 4: AWS ECS Fargate (Recommended for AWS)

Create `task-definition.json`:
```json
{
  "family": "sqs-consumer",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "containerDefinitions": [
    {
      "name": "sqs-consumer",
      "image": "YOUR_ECR_REPO/sqs-consumer:latest",
      "essential": true,
      "environment": [
        {
          "name": "ENABLE_SQS_CONSUMER",
          "value": "true"
        },
        {
          "name": "SQS_MAX_MESSAGES",
          "value": "10"
        },
        {
          "name": "SQS_WAIT_TIME_SECONDS",
          "value": "20"
        }
      ],
      "secrets": [
        {
          "name": "AWS_ACCESS_KEY_ID",
          "valueFrom": "arn:aws:secretsmanager:..."
        },
        {
          "name": "AWS_SECRET_ACCESS_KEY",
          "valueFrom": "arn:aws:secretsmanager:..."
        }
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/sqs-consumer",
          "awslogs-region": "ap-south-1",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
```

Deploy with Auto Scaling:
```bash
# Create ECS service with 15 tasks
aws ecs create-service \
  --cluster your-cluster \
  --service-name sqs-consumer \
  --task-definition sqs-consumer \
  --desired-count 15 \
  --launch-type FARGATE \
  --network-configuration "awsvpcConfiguration={subnets=[subnet-xxx],securityGroups=[sg-xxx]}"

# Set up auto-scaling
aws application-autoscaling register-scalable-target \
  --service-namespace ecs \
  --resource-id service/your-cluster/sqs-consumer \
  --scalable-dimension ecs:service:DesiredCount \
  --min-capacity 10 \
  --max-capacity 30
```

**Use Case**: Production AWS deployments  
**Pros**: Auto-scaling, managed, highly available  
**Cons**: AWS-specific, more complex setup

---

### Option 5: Kubernetes (Enterprise)

Create `deployment.yaml`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: sqs-consumer
  labels:
    app: sqs-consumer
spec:
  replicas: 15
  selector:
    matchLabels:
      app: sqs-consumer
  template:
    metadata:
      labels:
        app: sqs-consumer
    spec:
      containers:
      - name: sqs-consumer
        image: your-registry/sqs-consumer:latest
        env:
        - name: ENABLE_SQS_CONSUMER
          value: "true"
        - name: SQS_MAX_MESSAGES
          value: "10"
        - name: SQS_WAIT_TIME_SECONDS
          value: "20"
        - name: WORKER_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        envFrom:
        - secretRef:
            name: sqs-consumer-secrets
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: sqs-consumer-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: sqs-consumer
  minReplicas: 10
  maxReplicas: 30
  metrics:
  - type: External
    external:
      metric:
        name: sqs_queue_messages
      target:
        type: AverageValue
        averageValue: "100000"
```

Deploy:
```bash
kubectl apply -f deployment.yaml
```

**Use Case**: Large-scale, multi-cloud  
**Pros**: Highly scalable, cloud-agnostic, production-grade  
**Cons**: Complex, requires K8s expertise

---

## 📈 Auto-Scaling Configuration

### Metrics to Monitor

1. **ApproximateNumberOfMessages** (Queue Depth)
   - Alert if > 500,000
   - Scale up if > 1,000,000

2. **ApproximateAgeOfOldestMessage**
   - Alert if > 5 minutes
   - Scale up if > 10 minutes

3. **NumberOfMessagesReceived** (Incoming Rate)
   - Track daily/hourly trends

4. **Worker Throughput** (from logs)
   - Messages processed per second/hour/day

### CloudWatch Alarms (AWS)

```bash
# Alarm: Queue depth too high
aws cloudwatch put-metric-alarm \
  --alarm-name sqs-queue-depth-high \
  --alarm-description "SQS queue depth exceeds threshold" \
  --metric-name ApproximateNumberOfMessagesVisible \
  --namespace AWS/SQS \
  --statistic Average \
  --period 300 \
  --evaluation-periods 2 \
  --threshold 1000000 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=QueueName,Value=appflyer-queue

# Alarm: Message age too high
aws cloudwatch put-metric-alarm \
  --alarm-name sqs-message-age-high \
  --alarm-description "Oldest message age exceeds 5 minutes" \
  --metric-name ApproximateAgeOfOldestMessage \
  --namespace AWS/SQS \
  --statistic Maximum \
  --period 60 \
  --evaluation-periods 3 \
  --threshold 300 \
  --comparison-operator GreaterThanThreshold \
  --dimensions Name=QueueName,Value=appflyer-queue
```

### ECS Auto-Scaling Policy

```bash
# Scale based on queue depth
aws application-autoscaling put-scaling-policy \
  --service-namespace ecs \
  --resource-id service/your-cluster/sqs-consumer \
  --scalable-dimension ecs:service:DesiredCount \
  --policy-name sqs-queue-depth-scaling \
  --policy-type TargetTrackingScaling \
  --target-tracking-scaling-policy-configuration '{
    "TargetValue": 50000.0,
    "CustomizedMetricSpecification": {
      "MetricName": "ApproximateNumberOfMessagesVisible",
      "Namespace": "AWS/SQS",
      "Dimensions": [
        {
          "Name": "QueueName",
          "Value": "appflyer-queue"
        }
      ],
      "Statistic": "Average"
    },
    "ScaleInCooldown": 300,
    "ScaleOutCooldown": 60
  }'
```

**Scaling Logic**:
- If queue depth > 50k messages per worker → Scale OUT
- If queue depth < 50k messages per worker → Scale IN
- Min workers: 10
- Max workers: 30

---

## 📊 Monitoring & Observability

### Built-in Metrics (From Consumer Logs)

Every 60 seconds, each worker logs:
```json
{
  "workerId": "worker-1",
  "messagesProcessed": 1250,
  "messagesFailed": 12,
  "uptimeSeconds": 3600,
  "throughputPerSecond": "0.35",
  "throughputPerHour": 1250,
  "throughputPerDay": 30000
}
```

### Aggregated Dashboard Queries

**Total throughput across all workers**:
```
sum(throughputPerHour) across all workers
```

**Average processing rate**:
```
avg(throughputPerSecond) across all workers
```

**Failure rate**:
```
sum(messagesFailed) / sum(messagesProcessed) * 100
```

### Grafana Dashboard Example

Create a dashboard tracking:
1. Queue depth (live)
2. Total messages processed (hourly)
3. Processing rate per worker
4. Failed messages
5. Oldest message age

---

## 🔥 Emergency Actions

### Situation: Queue Growing Out of Control

**Immediate Actions**:

1. **Scale workers aggressively**
   ```bash
   # ECS
   aws ecs update-service --service sqs-consumer --desired-count 25
   
   # PM2
   pm2 scale sqs-consumer 25
   
   # Docker
   docker-compose up -d --scale sqs-consumer=25
   ```

2. **Check Redshift performance**
   - Is Redshift the bottleneck?
   - Check query execution times
   - Consider batch inserts if possible

3. **Increase visibility timeout**
   - Prevents message re-delivery during processing
   - Set to 2x your max processing time

4. **Temporarily increase worker resources**
   - More CPU/memory if processing is CPU-bound

---

## ✅ Success Criteria

### Short-term (Week 1)
- [ ] Deploy 15-20 workers
- [ ] Queue depth decreasing daily
- [ ] Throughput > 800k messages/day
- [ ] Age of oldest message < 5 minutes

### Medium-term (Week 2-4)
- [ ] Backlog cleared
- [ ] Queue depth stable < 100k
- [ ] Auto-scaling working
- [ ] Monitoring dashboard live

### Long-term (Ongoing)
- [ ] Throughput matches incoming rate (600k/day)
- [ ] Queue depth < 10k during normal operation
- [ ] Alerts configured
- [ ] Zero message loss

---

## 🎯 Final Recommendations

### Immediate (Today)
1. ✅ Code optimizations applied (polling interval removed, batch delete)
2. 🚀 Deploy 15 workers immediately
3. 📊 Monitor throughput for 24 hours

### Short-term (This Week)
1. Set up auto-scaling based on queue depth
2. Configure CloudWatch alarms
3. Create monitoring dashboard
4. Test scale-up/scale-down

### Long-term (Next Month)
1. Optimize Redshift insert performance
2. Consider Lambda for bursts
3. Implement DLQ (Dead Letter Queue) for failed messages
4. Set up log aggregation (CloudWatch Logs Insights, ELK)

---

## 💰 Cost Estimation

### SQS Costs
- Requests: ~$0.40 per million after free tier
- With 800k messages/day = 800k receives + 800k deletes = 1.6M requests/day
- Cost: ~$0.64/day = ~$19/month

### EC2/ECS Costs (15 workers)
- Fargate (512 CPU, 1GB RAM): ~$0.05/hour per task
- 15 tasks × $0.05 × 24 hours × 30 days = ~$540/month

### Alternative: Lambda
- 800k invocations/day × 3 seconds avg = 2.4M GB-seconds/day
- Cost: ~$40/month
- **But**: Lambda has cold starts, less control

**Verdict**: ECS/EC2 is worth it for consistent throughput.

---

## 🔗 Additional Resources

- [AWS SQS Best Practices](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-best-practices.html)
- [SQS Long Polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-short-and-long-polling.html)
- [ECS Auto Scaling](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/service-auto-scaling.html)

---

## 🆘 Need Help?

If your queue is still growing after implementing these changes:

1. Check Redshift query performance (likely bottleneck)
2. Profile your message processing time
3. Consider parallelizing Redshift writes
4. Add more workers (20 → 30)
5. Consider alternative architectures (Lambda, Kinesis, etc.)

**Remember**: The math doesn't lie. You need enough workers to process more than you receive.

