import 'reflect-metadata';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import dotenv from 'dotenv';
import { Container } from 'typedi';
import { SQSService } from './services/sqs.service';
import { SQSConsumer } from './workers/sqs-consumer';

// Load environment variables
dotenv.config();

const PORT = parseInt(process.env.PORT || '3000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const ENABLE_CONSUMER = process.env.ENABLE_SQS_CONSUMER === 'true';

// Create Fastify instance
const fastify = Fastify({
  logger: {
    level: process.env.LOG_LEVEL || 'info',
  },
});

// Get services from DI Container
const sqsService = Container.get(SQSService);
let sqsConsumer: SQSConsumer | null = null;

// Register CORS
fastify.register(cors, {
  origin: true,
});

// Health check endpoint
fastify.get('/health', async (request, reply) => {
  return { 
    status: 'ok', 
    timestamp: new Date().toISOString(),
    consumer: ENABLE_CONSUMER ? 'enabled' : 'disabled'
  };
});

// AppsFlyer webhook endpoint
interface AppsFlyerWebhookBody {
  event_name?: string;
  event_time?: string;
  app_id?: string;
  customer_user_id?: string;
  idfa?: string;
  advertising_id?: string;
  [key: string]: any;
}

fastify.post<{ Body: AppsFlyerWebhookBody }>(
  '/webhook/appsflyer',
  async (request, reply) => {
    try {
      const appsflyerData = request.body;
      
      fastify.log.info({
        msg: 'Received AppsFlyer webhook',
        eventName: appsflyerData.event_name,
        timestamp: appsflyerData.event_time,
      });

      // Validate webhook data
      if (!appsflyerData || Object.keys(appsflyerData).length === 0) {
        reply.code(400);
        return { 
          success: false, 
          error: 'Invalid webhook payload' 
        };
      }

      // Push data to SQS
      const messageId = await sqsService.pushToQueue(appsflyerData);
      
      fastify.log.info({
        msg: 'Data pushed to SQS',
        messageId,
      });

      reply.code(200);
      return {
        success: true,
        messageId,
        timestamp: new Date().toISOString(),
      };
    } catch (error) {
      fastify.log.error({
        msg: 'Error processing AppsFlyer webhook',
        error,
      });

      reply.code(500);
      return {
        success: false,
        error: 'Failed to process webhook',
      };
    }
  }
);

// Example API route (for testing)
fastify.get('/api/hello', async (request, reply) => {
  return { 
    message: 'AppsFlyer to Redshift Pipeline Service',
    endpoints: {
      health: 'GET /health',
      webhook: 'POST /webhook/appsflyer',
    },
  };
});

// Graceful shutdown
const gracefulShutdown = async () => {
  fastify.log.info('Shutting down gracefully...');
  
  if (sqsConsumer) {
    sqsConsumer.stop();
  }
  
  await fastify.close();
  process.exit(0);
};

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// Start server
const start = async () => {
  try {
    await fastify.listen({ port: PORT, host: HOST });
    fastify.log.info(`Server listening on ${HOST}:${PORT}`);

    // Start SQS consumer if enabled
    if (ENABLE_CONSUMER) {
      sqsConsumer = Container.get(SQSConsumer);
      await sqsConsumer.start();
      fastify.log.info('SQS Consumer started');
    } else {
      fastify.log.info('SQS Consumer is disabled. Set ENABLE_SQS_CONSUMER=true to enable.');
    }
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
