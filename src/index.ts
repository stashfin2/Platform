import 'reflect-metadata';
import Fastify from 'fastify';
import cors from '@fastify/cors';
import dotenv from 'dotenv';
import { Container } from 'typedi';
import { S3RedshiftSync } from './workers/s3-redshift-sync';
import { CleverTapSync } from './workers/clevertap-sync';
import { S3Service } from './services/s3.service';
import { registerRoutes } from './routes';

// Load environment variables
dotenv.config();

const PORT = parseInt(process.env.PORT || '6000', 10);
const HOST = process.env.HOST || '0.0.0.0';
const ENABLE_S3_SYNC = process.env.ENABLE_S3_SYNC === 'true';
const ENABLE_CLEVERTAP_SYNC = process.env.ENABLE_CLEVERTAP_SYNC === 'true';

// Create Fastify instance
const fastify = Fastify({
  logger: {
    level: process.env.LOG_LEVEL || 'info',
  },
});

let s3RedshiftSync: S3RedshiftSync | null = null;
let clevertapSync: CleverTapSync | null = null;
let s3Service: S3Service | null = null;

// Register CORS
fastify.register(cors, {
  origin: true,
});

// Register all routes
fastify.register(registerRoutes);

// Graceful shutdown
const gracefulShutdown = async () => {
  fastify.log.info('Shutting down gracefully...');
  
  // Force flush any pending S3 batches
  if (s3Service) {
    await s3Service.forceFlush();
  }
  
  // Stop S3 sync worker
  if (s3RedshiftSync) {
    s3RedshiftSync.stop();
  }
  
  // Stop CleverTap sync worker
  if (clevertapSync) {
    clevertapSync.stop();
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

    // Get S3Service instance for graceful shutdown
    s3Service = Container.get(S3Service);

    // Start S3 to Redshift sync worker if enabled
    if (ENABLE_S3_SYNC) {
      s3RedshiftSync = Container.get(S3RedshiftSync);
      await s3RedshiftSync.start();
      fastify.log.info('S3 to Redshift sync worker started');
    } else {
      fastify.log.info('S3 sync is disabled. Set ENABLE_S3_SYNC=true to enable.');
    }

    // Start CleverTap to Redshift sync worker if enabled
    if (ENABLE_CLEVERTAP_SYNC) {
      clevertapSync = Container.get(CleverTapSync);
      await clevertapSync.start();
      fastify.log.info('CleverTap to Redshift sync worker started');
    } else {
      fastify.log.info('CleverTap sync is disabled. Set ENABLE_CLEVERTAP_SYNC=true to enable.');
    }
  } catch (err) {
    fastify.log.error(err);
    process.exit(1);
  }
};

start();
