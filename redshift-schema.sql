-- AppsFlyer Events Table Schema for Redshift
-- Run this manually in your Redshift database

CREATE TABLE IF NOT EXISTS appsflyer_events (
  -- Primary identifiers
  appsflyer_id VARCHAR(255) PRIMARY KEY,
  event_name VARCHAR(255),
  event_time TIMESTAMP,
  event_time_selected_timezone TIMESTAMP,
  
  -- User identifiers
  customer_user_id VARCHAR(255),
  idfa VARCHAR(255),
  idfv VARCHAR(255),
  advertising_id VARCHAR(255),
  android_id VARCHAR(255),
  oaid VARCHAR(255),
  amazon_aid VARCHAR(255),
  imei VARCHAR(255),
  
  -- App information
  app_id VARCHAR(255),
  app_name VARCHAR(255),
  app_version VARCHAR(255),
  bundle_id VARCHAR(255),
  platform VARCHAR(50),
  sdk_version VARCHAR(100),
  api_version VARCHAR(50),
  
  -- Device information
  device_type VARCHAR(255),
  device_model VARCHAR(255),
  device_category VARCHAR(100),
  os_version VARCHAR(100),
  language VARCHAR(50),
  user_agent TEXT,
  
  -- Location information
  country_code VARCHAR(10),
  region VARCHAR(100),
  state VARCHAR(100),
  city VARCHAR(255),
  postal_code VARCHAR(50),
  dma VARCHAR(100),
  ip VARCHAR(45),
  
  -- Network information
  carrier VARCHAR(255),
  operator VARCHAR(255),
  wifi BOOLEAN,
  
  -- Attribution information
  media_source VARCHAR(255),
  campaign VARCHAR(500),
  campaign_type VARCHAR(255),
  af_channel VARCHAR(255),
  af_keywords VARCHAR(500),
  af_ad VARCHAR(500),
  af_ad_id VARCHAR(255),
  af_ad_type VARCHAR(255),
  af_adset VARCHAR(500),
  af_adset_id VARCHAR(255),
  af_c_id VARCHAR(255),
  af_siteid VARCHAR(255),
  af_sub_siteid VARCHAR(255),
  
  -- Sub parameters
  af_sub1 VARCHAR(500),
  af_sub2 VARCHAR(500),
  af_sub3 VARCHAR(500),
  af_sub4 VARCHAR(500),
  af_sub5 VARCHAR(500),
  
  -- Attribution details
  attributed_touch_type VARCHAR(255),
  attributed_touch_time TIMESTAMP,
  attributed_touch_time_selected_timezone TIMESTAMP,
  match_type VARCHAR(255),
  engagement_type VARCHAR(255),
  af_prt VARCHAR(255),
  af_attribution_lookback VARCHAR(255),
  af_reengagement_window VARCHAR(255),
  is_primary_attribution BOOLEAN,
  is_retargeting BOOLEAN,
  retargeting_conversion_type VARCHAR(255),
  
  -- Install information
  install_time TIMESTAMP,
  install_time_selected_timezone TIMESTAMP,
  install_app_store VARCHAR(255),
  device_download_time TIMESTAMP,
  device_download_time_selected_timezone TIMESTAMP,
  store_reinstall BOOLEAN,
  
  -- Contributors (for multi-touch attribution)
  contributor_1_media_source VARCHAR(255),
  contributor_1_campaign VARCHAR(500),
  contributor_1_touch_type VARCHAR(255),
  contributor_1_touch_time TIMESTAMP,
  contributor_1_match_type VARCHAR(255),
  contributor_1_engagement_type VARCHAR(255),
  contributor_1_af_prt VARCHAR(255),
  
  contributor_2_media_source VARCHAR(255),
  contributor_2_campaign VARCHAR(500),
  contributor_2_touch_type VARCHAR(255),
  contributor_2_touch_time TIMESTAMP,
  contributor_2_match_type VARCHAR(255),
  contributor_2_engagement_type VARCHAR(255),
  contributor_2_af_prt VARCHAR(255),
  
  contributor_3_media_source VARCHAR(255),
  contributor_3_campaign VARCHAR(500),
  contributor_3_touch_type VARCHAR(255),
  contributor_3_touch_time TIMESTAMP,
  contributor_3_match_type VARCHAR(255),
  contributor_3_engagement_type VARCHAR(255),
  contributor_3_af_prt VARCHAR(255),
  
  -- Event information
  event_type VARCHAR(255),
  event_source VARCHAR(100),
  event_value SUPER,  -- JSON data
  event_revenue DECIMAL(18,6),
  event_revenue_usd DECIMAL(18,6),
  event_revenue_currency VARCHAR(10),
  revenue_in_selected_currency DECIMAL(18,6),
  selected_currency VARCHAR(10),
  is_receipt_validated BOOLEAN,
  conversion_type VARCHAR(255),
  
  -- Cost information
  af_cost_model VARCHAR(255),
  af_cost_value DECIMAL(18,6),
  af_cost_currency VARCHAR(10),
  cost_in_selected_currency DECIMAL(18,6),
  
  -- Google Play referrer data
  gp_referrer TEXT,
  gp_click_time TIMESTAMP,
  gp_install_begin TIMESTAMP,
  gp_broadcast_referrer TEXT,
  
  -- Additional data
  http_referrer TEXT,
  original_url TEXT,
  deeplink_url TEXT,
  custom_data TEXT,
  custom_dimension VARCHAR(500),
  keyword_id VARCHAR(255),
  keyword_match_type VARCHAR(255),
  network_account_id VARCHAR(255),
  store_product_page TEXT,
  
  -- Privacy and consent
  is_lat BOOLEAN,
  gdpr_applies BOOLEAN,
  ad_personalization_enabled BOOLEAN,
  ad_user_data_enabled BOOLEAN,
  raw_consent_data TEXT,
  
  -- Timezone
  selected_timezone VARCHAR(100),
  
  -- Metadata
  created_at TIMESTAMP DEFAULT GETDATE(),
  updated_at TIMESTAMP DEFAULT GETDATE()
)
SORTKEY (event_time, appsflyer_id)
DISTKEY (customer_user_id);

