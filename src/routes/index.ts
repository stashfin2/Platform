import { FastifyInstance } from 'fastify';
import { healthRoutes } from './health.routes';
import { appsflyerRoutes } from './appsflyer.routes';

/**
 * Register all application routes
 */
export async function registerRoutes(fastify: FastifyInstance): Promise<void> {
  // Register health routes (no prefix)
  await fastify.register(healthRoutes);

  // Register AppsFlyer routes under /api prefix
  await fastify.register(appsflyerRoutes, { prefix: '/api' });
}

