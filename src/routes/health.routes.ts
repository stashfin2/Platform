import { FastifyInstance } from 'fastify';
import { Container } from 'typedi';
import { HealthController } from '../controllers/health.controller';

export async function healthRoutes(fastify: FastifyInstance): Promise<void> {
  const controller = Container.get(HealthController);

  // Health check endpoint
  fastify.get('/health', {
    schema: {
      description: 'Health check endpoint',
      tags: ['Health'],
      response: {
        200: {
          description: 'Successful response',
          type: 'object',
          properties: {
            status: { type: 'string' },
            timestamp: { type: 'string' },
            service: { type: 'string' },
            consumer: { type: 'string' },
            version: { type: 'string' },
          },
        },
      },
    },
    handler: controller.healthCheck.bind(controller),
  });

  // API info endpoint
  fastify.get('/api/info', {
    schema: {
      description: 'API information endpoint',
      tags: ['Health'],
      response: {
        200: {
          description: 'Successful response',
          type: 'object',
          properties: {
            message: { type: 'string' },
            version: { type: 'string' },
            endpoints: { type: 'object' },
          },
        },
      },
    },
    handler: controller.apiInfo.bind(controller),
  });
}

