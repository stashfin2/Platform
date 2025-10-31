# Platform Service - AppsFlyer to Redshift Data Pipeline

A Node.js service built with TypeScript and Fastify that processes AppsFlyer webhook events and stores them in AWS Redshift via SQS.

## Architecture Overview

```
AppsFlyer → POST /webhook/appsflyer → SQS Queue → SQS Consumer → Primary Redshift
                                                                  ↘ Secondary Redshift (optional)
```

### Data Flow

1. **AppsFlyer Webhook** - AppsFlyer publishes event data to `/webhook/appsflyer` endpoint
2. **SQS Producer** - Service receives data and pushes it to an AWS SQS queue
3. **SQS Consumer** - Background worker polls SQS for new messages
4. **Redshift Storage** - Consumer processes messages and inserts data into Redshift
   - Primary Redshift (required): Main data warehouse
   - Secondary Redshift (optional): Dual-write to a second Redshift in different VPC via PrivateLink

## Features

- 🚀 **Fastify** - Fast and low overhead web framework
- 📘 **TypeScript** - Type-safe development
- 🔄 **Hot Reload** - Auto-restart on file changes with ts-node-dev
- 🌐 **CORS** - Configured with @fastify/cors
- 📝 **Logging** - Built-in request logging with Pino
- ⚙️ **Environment Variables** - Configuration via .env files
- ☁️ **AWS Integration** - SQS and Redshift Data API
- 🔁 **Background Worker** - Separate SQS consumer process
- 🛡️ **Error Handling** - Robust error handling and retry logic
- 🔁 **Dual-Write Support** - Optional parallel writes to secondary Redshift cluster via PrivateLink

## Prerequisites

- Node.js (v16 or higher)
- npm or yarn
- AWS Account with:
  - SQS Queue created
  - Redshift cluster configured
  - IAM credentials with appropriate permissions

## AWS Setup

### Required IAM Permissions

Your AWS credentials need the following permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:*:*:appsflyer-events"
    },
    {
      "Effect": "Allow",
      "Action": [
        "redshift-data:ExecuteStatement",
        "redshift-data:DescribeStatement"
      ],
      "Resource": "*"
    }
  ]
}
```

### Create SQS Queue

```bash
aws sqs create-queue --queue-name appsflyer-events --region us-east-1
```

### Redshift Table Schema

The service will automatically create the table, but you can also create it manually:

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

## Getting Started

### 1. Install Dependencies

```bash
npm install
```

### 2. Configure Environment

Create a `.env` file in the root directory:

```env
# Server Configuration
PORT=3000
HOST=0.0.0.0
LOG_LEVEL=info

# SQS Consumer Configuration
ENABLE_SQS_CONSUMER=true
SQS_POLLING_INTERVAL=5000

# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_id
AWS_SECRET_ACCESS_KEY=your_secret_access_key

# AWS SQS Configuration
SQS_QUEUE_URL=https://sqs.us-east-1.amazonaws.com/123456789012/appsflyer-events

# Primary AWS Redshift Configuration
REDSHIFT_CLUSTER_IDENTIFIER=your-redshift-cluster
REDSHIFT_DATABASE=your_database
REDSHIFT_DB_USER=your_db_user
REDSHIFT_TABLE_NAME=appsflyer_events
REDSHIFT_STATEMENT_TIMEOUT=60000
REDSHIFT_MAX_RETRIES=30

# Secondary AWS Redshift Configuration (Optional - for dual-write across VPCs)
SECONDARY_REDSHIFT_ENABLED=false
# SECONDARY_REDSHIFT_ENDPOINT=https://akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com
# SECONDARY_AWS_REGION=ap-south-1
# SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER=your-secondary-redshift-cluster
# SECONDARY_REDSHIFT_DATABASE=your_secondary_database
# SECONDARY_REDSHIFT_DB_USER=your_secondary_db_user
# SECONDARY_REDSHIFT_TABLE_NAME=appsflyer_events
# SECONDARY_REDSHIFT_STATEMENT_TIMEOUT=60000
# SECONDARY_REDSHIFT_MAX_RETRIES=30
```

### 3. Run Development Server

```bash
npm run dev
```

The server will start on `http://localhost:3000`

### 4. Build for Production

```bash
npm run build
```

### 5. Start Production Server

```bash
npm start
```

## Available Scripts

- `npm run dev` - Start development server with hot reload
- `npm run build` - Build TypeScript to JavaScript
- `npm start` - Start production server
- `npm run lint` - Lint code with ESLint
- `npm run format` - Format code with Prettier

## API Endpoints

### Health Check
```
GET /health
```
Returns the health status of the service and consumer status.

**Response:**
```json
{
  "status": "ok",
  "timestamp": "2024-01-01T00:00:00.000Z",
  "consumer": "enabled"
}
```

### AppsFlyer Webhook
```
POST /webhook/appsflyer
Content-Type: application/json
```

Receives event data from AppsFlyer and pushes it to SQS.

**Request Body Example:**
```json
{
  "event_name": "af_purchase",
  "event_time": "2024-01-01 12:00:00.000",
  "app_id": "com.example.app",
  "customer_user_id": "user123",
  "idfa": "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
  "event_value": {
    "af_revenue": 9.99,
    "af_currency": "USD"
  }
}
```

**Response:**
```json
{
  "success": true,
  "messageId": "uuid-message-id",
  "timestamp": "2024-01-01T00:00:00.000Z"
}
```

### Hello API
```
GET /api/hello
```
Returns service information and available endpoints.

## Dual-Write to Secondary Redshift (Cross-VPC via PrivateLink)

The service supports optional dual-write functionality to replicate data to a secondary Redshift cluster in a different VPC using AWS PrivateLink.

### Features

- **Parallel Writes**: Data is inserted into both primary and secondary Redshift clusters simultaneously
- **Independent Failure Handling**: If one Redshift insert fails, the other still completes successfully
- **PrivateLink Support**: Connect to Redshift in different VPC using VPC endpoint
- **Graceful Degradation**: Service continues working even if secondary Redshift is unavailable
- **Comprehensive Logging**: Separate logs for primary and secondary insert operations

### Configuration

To enable dual-write to a secondary Redshift:

1. **Set up AWS PrivateLink** (if cross-VPC):
   - Create a VPC endpoint for Redshift Data API in your VPC
   - Note the endpoint URL (e.g., `https://vpce-xxxxx.redshift-data.us-west-2.vpce.amazonaws.com`)

2. **Enable Secondary Redshift** in your `.env`:
   ```env
   SECONDARY_REDSHIFT_ENABLED=true
   SECONDARY_REDSHIFT_ENDPOINT=https://akara-redshift-access-appflyer-endpoint-fxsovvcwarpoattqgxt0.churfikvu1nn.ap-south-1.redshift.amazonaws.com
   SECONDARY_AWS_REGION=ap-south-1
   SECONDARY_REDSHIFT_CLUSTER_IDENTIFIER=your-secondary-cluster
   SECONDARY_REDSHIFT_DATABASE=your_database
   SECONDARY_REDSHIFT_DB_USER=your_user
   SECONDARY_REDSHIFT_TABLE_NAME=appsflyer_events
   ```

3. **Ensure IAM Permissions** for both Redshift clusters:
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

### How It Works

1. When a message is consumed from SQS, the service:
   - Parses the AppsFlyer event data
   - Creates two parallel insert operations (primary + secondary)
   - Executes both inserts using `Promise.allSettled()`
   
2. Each insert operation:
   - Connects to its respective Redshift cluster
   - Builds and executes the INSERT statement
   - Waits for statement completion
   - Logs success or failure independently

3. Success criteria:
   - Primary insert must succeed (returns statement ID)
   - Secondary insert failure is logged but doesn't fail the overall operation
   - SQS message is only deleted if primary insert succeeds

### Monitoring

Monitor dual-write operations in logs:

```json
{
  "msg": "Data successfully inserted into primary Redshift",
  "statementId": "uuid-xxx",
  "target": "primary"
}
{
  "msg": "Data successfully inserted into secondary Redshift",
  "statementId": "uuid-yyy",
  "target": "secondary"
}
```

If secondary insert fails:
```json
{
  "level": "error",
  "msg": "Failed to insert data into secondary Redshift",
  "target": "secondary",
  "error": "..."
}
```

### Disabling Secondary Redshift

To disable dual-write at any time:
```env
SECONDARY_REDSHIFT_ENABLED=false
```

The service will continue operating normally with only the primary Redshift.

## Project Structure

```
platform/
├── src/
│   ├── index.ts              # Main application entry point
│   ├── services/
│   │   ├── sqs.service.ts    # SQS producer/consumer service
│   │   └── redshift.service.ts  # Redshift data insertion service
│   └── workers/
│       └── sqs-consumer.ts   # SQS consumer background worker
├── dist/                     # Compiled JavaScript (generated)
├── node_modules/             # Dependencies (generated)
├── .gitignore               # Git ignore rules
├── package.json             # Project dependencies and scripts
├── tsconfig.json            # TypeScript configuration
└── README.md                # This file
```

## Development

### Testing the AppsFlyer Webhook

You can test the webhook endpoint using curl:

```bash
curl -X POST http://localhost:3000/webhook/appsflyer \
  -H "Content-Type: application/json" \
  -d '{
    "event_name": "test_event",
    "event_time": "2024-01-01 12:00:00",
    "app_id": "com.test.app",
    "customer_user_id": "test_user_123"
  }'
```

### Running Without SQS Consumer

If you only want to run the API without the background consumer:

```env
ENABLE_SQS_CONSUMER=false
```

This is useful for development or when running the consumer as a separate process.

### Monitoring

The service uses Fastify's built-in Pino logger. All events are logged with structured data:

- Incoming webhook requests
- SQS message publishing
- SQS message consumption
- Redshift data insertion
- Errors and exceptions

## Deployment

### Docker Deployment (Recommended)

Create a `Dockerfile`:

```dockerfile
FROM node:20-alpine

WORKDIR /app

COPY package*.json ./
RUN npm ci --only=production

COPY . .
RUN npm run build

EXPOSE 3000

CMD ["npm", "start"]
```

Build and run:

```bash
docker build -t platform-service .
docker run -p 3000:3000 --env-file .env platform-service
```

### AWS ECS / Fargate

The service is designed to run on AWS ECS/Fargate:

1. Build and push Docker image to ECR
2. Create ECS task definition with environment variables
3. Deploy as ECS service with appropriate security groups
4. Configure Application Load Balancer to route AppsFlyer webhooks

### Environment Variables in Production

Use AWS Systems Manager Parameter Store or AWS Secrets Manager for sensitive credentials:

```javascript
// Example: Load from AWS SSM Parameter Store
const AWS = require('aws-sdk');
const ssm = new AWS.SSM();

async function loadParameter(name) {
  const result = await ssm.getParameter({
    Name: name,
    WithDecryption: true
  }).promise();
  return result.Parameter.Value;
}
```

## Scaling

### Horizontal Scaling

- **API Service**: Scale multiple instances behind a load balancer
- **Consumer Service**: Run multiple consumer instances to process SQS messages in parallel

### SQS Configuration

- Configure appropriate **visibility timeout** (should be longer than processing time)
- Set **dead letter queue** for failed messages
- Adjust **max receive count** for retry logic

## Monitoring & Observability

### CloudWatch Metrics

Monitor these key metrics:

- API request count and latency
- SQS queue depth
- Message processing time
- Redshift insertion success/failure rate

### Logging

All logs are JSON formatted and can be shipped to:
- CloudWatch Logs
- Elasticsearch
- Datadog
- Splunk

## Troubleshooting

### Common Issues

1. **SQS Messages Not Being Processed**
   - Check `ENABLE_SQS_CONSUMER=true` in environment
   - Verify AWS credentials have SQS permissions
   - Check SQS queue URL is correct

2. **Redshift Insertion Failures**
   - Verify Redshift cluster is running
   - Check database credentials
   - Ensure table exists or service has permission to create it

3. **AppsFlyer Webhook Not Working**
   - Verify endpoint is publicly accessible
   - Check firewall/security group rules
   - Validate webhook payload format

## Security

- Never commit `.env` file with real credentials
- Use IAM roles instead of access keys when running on AWS
- Enable VPC endpoints for SQS and Redshift
- Use HTTPS in production
- Implement webhook signature verification for AppsFlyer

## License

MIT
