import { FastifyInstance } from 'fastify';
import { Container } from 'typedi';
import { AppsFlyerController } from '../controllers/appsflyer.controller';

export async function appsflyerRoutes(fastify: FastifyInstance): Promise<void> {
  const controller = Container.get(AppsFlyerController);

  // AppsFlyer webhook endpoint
  fastify.post('/webhook/appsflyer', {
    schema: {
      description: 'AppsFlyer webhook endpoint - accepts complete AppsFlyer payload',
      tags: ['AppsFlyer'],
      body: {
        type: 'object',
        additionalProperties: true, // Accept any additional properties
        required: ['appsflyer_id', 'event_name', 'event_time'], // Only enforce critical fields
        properties: {
          // Core required fields
          appsflyer_id: { type: 'string' },
          event_name: { type: 'string' },
          event_time: { type: 'string' },
          // All other fields are optional and can be of any type
        },
      },
      response: {
        200: {
          description: 'Successful response',
          type: 'object',
          properties: {
            success: { type: 'boolean' },
            messageId: { type: 'string' },
            timestamp: { type: 'string' },
          },
        },
        400: {
          description: 'Bad request',
          type: 'object',
          properties: {
            success: { type: 'boolean' },
            error: { type: 'string' },
          },
        },
        500: {
          description: 'Internal server error',
          type: 'object',
          properties: {
            success: { type: 'boolean' },
            error: { type: 'string' },
          },
        },
      },
    },
    handler: controller.handleWebhook.bind(controller),
  });
}
``
