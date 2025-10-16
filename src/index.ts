import 'reflect-metadata';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import dotenv from 'dotenv';
import { Container } from 'typedi';
import { SQSConsumer } from './workers/sqs-consumer';
import { registerRoutes } from './routes';

// Load environment variables
dotenv.config();

const PORT = parseInt(process.env.PORT || '6000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const ENABLE_CONSUMER = process.env.ENABLE_SQS_CONSUMER === 'true';

// Create Fastify instance
const fastify = Fastify({
  logger: {
    level: process.env.LOG_LEVEL || 'info',
  },
});

let sqsConsumer: SQSConsumer | null = null;

// Register CORS
fastify.register(cors, {
  origin: true,
});

// Register all routes
fastify.register(registerRoutes);

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
