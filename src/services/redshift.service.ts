import 'reflect-metadata';
import { Service, Inject } from 'typedi';
import { ExecuteStatementCommand, DescribeStatementCommand } from '@aws-sdk/client-redshift-data';
import { LoggerService } from './logger.service';
import { RedshiftClientFactory } from '../config/redshift.config';

interface AppsFlyerEvent {
  [key: string]: any;
}

@Service()
export class RedshiftService {
  constructor(
    @Inject() private readonly redshiftClientFactory: RedshiftClientFactory,
    @Inject() private readonly logger: LoggerService
  ) {}

  /**
   * Insert AppsFlyer event data into Redshift table
   */
  async insertData(data: AppsFlyerEvent): Promise<string> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();

    try {
      // Parse the data if it's a string
      const parsedData = typeof data === 'string' ? JSON.parse(data) : data;
      
      // Helper function to safely escape and format values
      const formatValue = (value: any): string => {
        if (value === null || value === undefined || value === '') {
          return 'NULL';
        }
        if (typeof value === 'boolean') {
          return value ? 'TRUE' : 'FALSE';
        }
        if (typeof value === 'number') {
          return String(value);
        }
        if (typeof value === 'object') {
          return `'${JSON.stringify(value).replace(/'/g, "''")}'`;
        }
        // String value - escape single quotes
        return `'${String(value).replace(/'/g, "''")}'`;
      };

      // Helper function to format SUPER (JSON) type values for Redshift
      const formatSuperValue = (value: any): string => {
        if (value === null || value === undefined || value === '') {
          return 'NULL';
        }
        if (typeof value === 'object') {
          return `JSON_PARSE('${JSON.stringify(value).replace(/'/g, "''")}')`;
        }
        if (typeof value === 'string') {
          // If it's already a JSON string, parse it
          try {
            JSON.parse(value);
            return `JSON_PARSE('${value.replace(/'/g, "''")}')`;
          } catch {
            // Not valid JSON, treat as a simple string value
            return `JSON_PARSE('"${value.replace(/"/g, '\\"').replace(/'/g, "''")}"')`;
          }
        }
        // For primitives, wrap in JSON_PARSE
        return `JSON_PARSE('${JSON.stringify(value).replace(/'/g, "''")}')`;
      };

      // Helper function to format timestamp
      const formatTimestamp = (value: any): string => {
        if (!value) return 'NULL';
        try {
          // AppsFlyer sends timestamps in format: "2020-01-15 14:57:24.898"
          return `'${value}'`;
        } catch {
          return 'NULL';
        }
      };

      // Build INSERT statement with all fields
      const sql = `
        INSERT INTO ${config.tableName} (
          appsflyer_id, event_name, event_time, event_time_selected_timezone,
          customer_user_id, idfa, idfv, advertising_id, android_id, oaid, amazon_aid, imei,
          app_id, app_name, app_version, bundle_id, platform, sdk_version, api_version,
          device_type, device_model, device_category, os_version, language, user_agent,
          country_code, region, state, city, postal_code, dma, ip,
          carrier, operator, wifi,
          media_source, campaign, campaign_type, af_channel, af_keywords, af_ad, af_ad_id, 
          af_ad_type, af_adset, af_adset_id, af_c_id, af_siteid, af_sub_siteid,
          af_sub1, af_sub2, af_sub3, af_sub4, af_sub5,
          attributed_touch_type, attributed_touch_time, attributed_touch_time_selected_timezone,
          match_type, engagement_type, af_prt, af_attribution_lookback, af_reengagement_window,
          is_primary_attribution, is_retargeting, retargeting_conversion_type,
          install_time, install_time_selected_timezone, install_app_store,
          device_download_time, device_download_time_selected_timezone, store_reinstall,
          contributor_1_media_source, contributor_1_campaign, contributor_1_touch_type,
          contributor_1_touch_time, contributor_1_match_type, contributor_1_engagement_type, contributor_1_af_prt,
          contributor_2_media_source, contributor_2_campaign, contributor_2_touch_type,
          contributor_2_touch_time, contributor_2_match_type, contributor_2_engagement_type, contributor_2_af_prt,
          contributor_3_media_source, contributor_3_campaign, contributor_3_touch_type,
          contributor_3_touch_time, contributor_3_match_type, contributor_3_engagement_type, contributor_3_af_prt,
          event_type, event_source, event_value, event_revenue, event_revenue_usd,
          event_revenue_currency, revenue_in_selected_currency, selected_currency, is_receipt_validated, conversion_type,
          af_cost_model, af_cost_value, af_cost_currency, cost_in_selected_currency,
          gp_referrer, gp_click_time, gp_install_begin, gp_broadcast_referrer,
          http_referrer, original_url, deeplink_url, custom_data, custom_dimension,
          keyword_id, keyword_match_type, network_account_id, store_product_page,
          is_lat, gdpr_applies, ad_personalization_enabled, ad_user_data_enabled, raw_consent_data,
          selected_timezone
        ) VALUES (
          ${formatValue(parsedData.appsflyer_id)}, ${formatValue(parsedData.event_name)}, 
          ${formatTimestamp(parsedData.event_time)}, ${formatTimestamp(parsedData.event_time_selected_timezone)},
          ${formatValue(parsedData.customer_user_id)}, ${formatValue(parsedData.idfa)}, ${formatValue(parsedData.idfv)}, 
          ${formatValue(parsedData.advertising_id)}, ${formatValue(parsedData.android_id)}, ${formatValue(parsedData.oaid)}, 
          ${formatValue(parsedData.amazon_aid)}, ${formatValue(parsedData.imei)},
          ${formatValue(parsedData.app_id)}, ${formatValue(parsedData.app_name)}, ${formatValue(parsedData.app_version)}, 
          ${formatValue(parsedData.bundle_id)}, ${formatValue(parsedData.platform)}, ${formatValue(parsedData.sdk_version)}, 
          ${formatValue(parsedData.api_version)},
          ${formatValue(parsedData.device_type)}, ${formatValue(parsedData.device_model)}, ${formatValue(parsedData.device_category)}, 
          ${formatValue(parsedData.os_version)}, ${formatValue(parsedData.language)}, ${formatValue(parsedData.user_agent)},
          ${formatValue(parsedData.country_code)}, ${formatValue(parsedData.region)}, ${formatValue(parsedData.state)}, 
          ${formatValue(parsedData.city)}, ${formatValue(parsedData.postal_code)}, ${formatValue(parsedData.dma)}, 
          ${formatValue(parsedData.ip)},
          ${formatValue(parsedData.carrier)}, ${formatValue(parsedData.operator)}, ${formatValue(parsedData.wifi)},
          ${formatValue(parsedData.media_source)}, ${formatValue(parsedData.campaign)}, ${formatValue(parsedData.campaign_type)}, 
          ${formatValue(parsedData.af_channel)}, ${formatValue(parsedData.af_keywords)}, ${formatValue(parsedData.af_ad)}, 
          ${formatValue(parsedData.af_ad_id)}, ${formatValue(parsedData.af_ad_type)}, ${formatValue(parsedData.af_adset)}, 
          ${formatValue(parsedData.af_adset_id)}, ${formatValue(parsedData.af_c_id)}, ${formatValue(parsedData.af_siteid)}, 
          ${formatValue(parsedData.af_sub_siteid)},
          ${formatValue(parsedData.af_sub1)}, ${formatValue(parsedData.af_sub2)}, ${formatValue(parsedData.af_sub3)}, 
          ${formatValue(parsedData.af_sub4)}, ${formatValue(parsedData.af_sub5)},
          ${formatValue(parsedData.attributed_touch_type)}, ${formatTimestamp(parsedData.attributed_touch_time)}, 
          ${formatTimestamp(parsedData.attributed_touch_time_selected_timezone)},
          ${formatValue(parsedData.match_type)}, ${formatValue(parsedData.engagement_type)}, ${formatValue(parsedData.af_prt)}, 
          ${formatValue(parsedData.af_attribution_lookback)}, ${formatValue(parsedData.af_reengagement_window)},
          ${formatValue(parsedData.is_primary_attribution)}, ${formatValue(parsedData.is_retargeting)}, 
          ${formatValue(parsedData.retargeting_conversion_type)},
          ${formatTimestamp(parsedData.install_time)}, ${formatTimestamp(parsedData.install_time_selected_timezone)}, 
          ${formatValue(parsedData.install_app_store)},
          ${formatTimestamp(parsedData.device_download_time)}, ${formatTimestamp(parsedData.device_download_time_selected_timezone)}, 
          ${formatValue(parsedData.store_reinstall)},
          ${formatValue(parsedData.contributor_1_media_source)}, ${formatValue(parsedData.contributor_1_campaign)}, 
          ${formatValue(parsedData.contributor_1_touch_type)}, ${formatTimestamp(parsedData.contributor_1_touch_time)}, 
          ${formatValue(parsedData.contributor_1_match_type)}, ${formatValue(parsedData.contributor_1_engagement_type)}, 
          ${formatValue(parsedData.contributor_1_af_prt)},
          ${formatValue(parsedData.contributor_2_media_source)}, ${formatValue(parsedData.contributor_2_campaign)}, 
          ${formatValue(parsedData.contributor_2_touch_type)}, ${formatTimestamp(parsedData.contributor_2_touch_time)}, 
          ${formatValue(parsedData.contributor_2_match_type)}, ${formatValue(parsedData.contributor_2_engagement_type)}, 
          ${formatValue(parsedData.contributor_2_af_prt)},
          ${formatValue(parsedData.contributor_3_media_source)}, ${formatValue(parsedData.contributor_3_campaign)}, 
          ${formatValue(parsedData.contributor_3_touch_type)}, ${formatTimestamp(parsedData.contributor_3_touch_time)}, 
          ${formatValue(parsedData.contributor_3_match_type)}, ${formatValue(parsedData.contributor_3_engagement_type)}, 
          ${formatValue(parsedData.contributor_3_af_prt)},
          ${formatValue(parsedData.event_type)}, ${formatValue(parsedData.event_source)}, ${formatSuperValue(parsedData.event_value)}, 
          ${formatValue(parsedData.event_revenue)}, ${formatValue(parsedData.event_revenue_usd)},
          ${formatValue(parsedData.event_revenue_currency)}, ${formatValue(parsedData.revenue_in_selected_currency)}, 
          ${formatValue(parsedData.selected_currency)}, ${formatValue(parsedData.is_receipt_validated)}, 
          ${formatValue(parsedData.conversion_type)},
          ${formatValue(parsedData.af_cost_model)}, ${formatValue(parsedData.af_cost_value)}, 
          ${formatValue(parsedData.af_cost_currency)}, ${formatValue(parsedData.cost_in_selected_currency)},
          ${formatValue(parsedData.gp_referrer)}, ${formatTimestamp(parsedData.gp_click_time)}, 
          ${formatTimestamp(parsedData.gp_install_begin)}, ${formatValue(parsedData.gp_broadcast_referrer)},
          ${formatValue(parsedData.http_referrer)}, ${formatValue(parsedData.original_url)}, ${formatValue(parsedData.deeplink_url)}, 
          ${formatValue(parsedData.custom_data)}, ${formatValue(parsedData.custom_dimension)},
          ${formatValue(parsedData.keyword_id)}, ${formatValue(parsedData.keyword_match_type)}, 
          ${formatValue(parsedData.network_account_id)}, ${formatValue(parsedData.store_product_page)},
          ${formatValue(parsedData.is_lat)}, ${formatValue(parsedData.gdpr_applies)}, 
          ${formatValue(parsedData.ad_personalization_enabled)}, ${formatValue(parsedData.ad_user_data_enabled)}, 
          ${formatValue(parsedData.raw_consent_data)},
          ${formatValue(parsedData.selected_timezone)}
        );
      `;

      const command = new ExecuteStatementCommand({
        ClusterIdentifier: config.clusterIdentifier,
        Database: config.database,
        DbUser: config.dbUser,
        Sql: sql,
      });

      const response = await client.send(command);
      const statementId = response.Id || '';
      
      this.logger.info('Data inserted into Redshift', {
        statementId,
        tableName: config.tableName,
        appsflyerId: parsedData.appsflyer_id,
        eventName: parsedData.event_name,
      });
      
      // Wait for statement to complete
      await this.waitForStatementCompletion(statementId);
      
      return statementId;
    } catch (error) {
      this.logger.error('Error inserting data into Redshift', error, {
        tableName: config.tableName,
        appsflyerId: data.appsflyer_id,
      });
      throw error;
    }
  }

  /**
   * Wait for Redshift statement to complete
   */
  private async waitForStatementCompletion(statementId: string): Promise<void> {
    const client = this.redshiftClientFactory.getClient();
    const config = this.redshiftClientFactory.getConfig();
    const startTime = Date.now();

    for (let i = 0; i < config.maxRetries; i++) {
      const command = new DescribeStatementCommand({ Id: statementId });
      const response = await client.send(command);
      
      const status = response.Status;
      
      if (i > 0 && i % 5 === 0) {
        // Log progress every 5 attempts (every 10 seconds)
        this.logger.debug('Waiting for Redshift statement completion', {
          statementId,
          status,
          attempt: i + 1,
          maxRetries: config.maxRetries,
        });
      }
      
      if (status === 'FINISHED') {
        const duration = Date.now() - startTime;
        this.logger.debug('Redshift statement completed successfully', {
          statementId,
          durationMs: duration,
          attempts: i + 1,
        });
        return;
      } else if (status === 'FAILED' || status === 'ABORTED') {
        const errorMessage = response.Error || 'Unknown error';
        this.logger.error('Redshift statement failed', new Error(errorMessage), {
          statementId,
          status,
        });
        throw new Error(`Redshift statement failed: ${errorMessage}`);
      }
      
      // Wait before next check (2 seconds)
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
    
    this.logger.error('Timeout waiting for Redshift statement to complete', undefined, {
      statementId,
      maxRetries: config.maxRetries,
    });
    throw new Error('Timeout waiting for Redshift statement to complete');
  }
}
