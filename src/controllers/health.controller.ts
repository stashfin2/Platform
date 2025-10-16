import 'reflect-metadata';
import { Service } from 'typedi';
import { FastifyRequest, FastifyReply } from 'fastify';

@Service()
export class HealthController {
  /**
   * Health check endpoint
   */
  async healthCheck(
    _request: FastifyRequest,
    reply: FastifyReply
  ): Promise<void> {
    const isConsumerEnabled = process.env.ENABLE_SQS_CONSUMER === 'true';

    reply.code(200).send({
      status: 'ok',
      timestamp: new Date().toISOString(),
      service: 'Platform Service',
      consumer: isConsumerEnabled ? 'enabled' : 'disabled',
      version: process.env.npm_package_version || '1.0.0',
    });
  }

  /**
   * API info endpoint
   */
  async apiInfo(
    _request: FastifyRequest,
    reply: FastifyReply
  ): Promise<void> {
    reply.code(200).send({
      message: 'AppsFlyer to Redshift Pipeline Service',
      version: '1.0.0',
      endpoints: {
        health: 'GET /health',
        apiInfo: 'GET /api/info',
        webhook: 'POST /api/webhook/appsflyer',
      },
    });
  }
}

