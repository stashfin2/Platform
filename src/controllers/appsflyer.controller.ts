import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { FastifyRequest, FastifyReply } from 'fastify';
import { SQSService } from '../services/sqs.service';
import { LoggerService } from '../services/logger.service';

export interface AppsFlyerWebhookBody {
  // Required fields (needed for Redshift)
  appsflyer_id: string;
  event_name: string;
  event_time: string;
  
  // Common optional fields
  customer_user_id?: string;
  event_time_selected_timezone?: string;
  idfa?: string;
  idfv?: string;
  advertising_id?: string;
  android_id?: string;
  app_id?: string;
  app_name?: string;
  platform?: string;
  media_source?: string;
  campaign?: string;
  
  // Accept any additional AppsFlyer fields
  [key: string]: any;
}

@Service()
export class AppsFlyerController {
  constructor(
    @Inject() private readonly sqsService: SQSService,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * Handle AppsFlyer webhook
   */
  async handleWebhook(
    request: FastifyRequest<{ Body: AppsFlyerWebhookBody }>,
    reply: FastifyReply
  ): Promise<void> {
    try {
      const appsflyerData = request.body;
      
      this.logger.info('Received AppsFlyer webhook', {
        appsflyerId: appsflyerData.appsflyer_id,
        eventName: appsflyerData.event_name,
        eventTime: appsflyerData.event_time,
        platform: appsflyerData.platform,
        appId: appsflyerData.app_id,
      });

      // Validate required fields (schema validation already done by Fastify)
      // This is a safety check in case schema validation is bypassed
      if (!appsflyerData || Object.keys(appsflyerData).length === 0) {
        this.logger.warn('Empty webhook payload received');
        reply.code(400).send({ 
          success: false, 
          error: 'Empty payload' 
        });
        return;
      }

      if (!appsflyerData.appsflyer_id || !appsflyerData.event_name || !appsflyerData.event_time) {
        this.logger.warn('Missing required fields in webhook', {
          hasAppsflyerId: !!appsflyerData.appsflyer_id,
          hasEventName: !!appsflyerData.event_name,
          hasEventTime: !!appsflyerData.event_time,
        });
        reply.code(400).send({ 
          success: false, 
          error: 'Missing required fields: appsflyer_id, event_name, or event_time' 
        });
        return;
      }

      // Push complete payload to SQS
      const messageId = await this.sqsService.pushToQueue(appsflyerData);
      
      this.logger.info('Data pushed to SQS successfully', {
        messageId,
        appsflyerId: appsflyerData.appsflyer_id,
        eventName: appsflyerData.event_name,
      });

      reply.code(200).send({
        success: true,
        messageId,
        timestamp: new Date().toISOString(),
      });
    } catch (error) {
      this.logger.error('Error processing AppsFlyer webhook', error, {
        hasBody: !!request.body,
      });

      reply.code(500).send({
        success: false,
        error: 'Failed to process webhook',
      });
    }
  }
}
