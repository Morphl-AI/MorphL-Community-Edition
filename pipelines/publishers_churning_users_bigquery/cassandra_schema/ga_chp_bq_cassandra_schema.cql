CREATE KEYSPACE IF NOT EXISTS morphl WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE morphl.ga_chp_bq_features_raw_t (
  client_id text,
  day_of_data_capture date,
  website_url text,
  sessions double,
  bounces double,
  events double,
  session_duration double,
  page_views double,
  is_mobile double,
  is_desktop double,
  is_tablet double,
  days_since_last_session double,
  session_dates list<text>,
  PRIMARY KEY ((client_id), day_of_data_capture, website_url)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_chp_bq_features_raw_p (
  client_id text,
  day_of_data_capture date,
  website_url text,
  sessions double,
  bounces double,
  events double,
  session_duration double,
  page_views double,
  is_mobile double,
  is_desktop double,
  is_tablet double,
  session_dates list<text>,
  PRIMARY KEY ((client_id), day_of_data_capture, website_url)
) WITH CLUSTERING ORDER BY (day_of_data_capture DESC);

CREATE TABLE morphl.ga_chp_bq_features_training (
  client_id text,
  bounces double,
  events double,
  page_views double,
  session_duration double,
  sessions double,
  is_desktop double,
  is_mobile double,
  is_tablet double,
  churned double,
  PRIMARY KEY ((client_id))
);

CREATE TABLE morphl.ga_chp_bq_features_prediction (
  client_id text,
  bounces double,
  events double,
  page_views double,
  session_duration double,
  sessions double,
  is_desktop double,
  is_mobile double,
  is_tablet double,
  PRIMARY KEY ((client_id))
);

CREATE TABLE morphl.ga_chp_bq_predictions (
  client_id text,
  prediction double,
  PRIMARY KEY ((client_id))
);

CREATE TABLE morphl.ga_chp_bq_predictions_by_prediction_date (
  prediction_date date,
  client_id text, 
  prediction double,
  PRIMARY KEY ((prediction_date), client_id)
);

CREATE TABLE morphl.ga_chp_bq_predictions_statistics (
  prediction_date date, 
  loyal counter,
  neutral counter,
  churning counter,
  lost counter,
  PRIMARY KEY ((prediction_date))
);

CREATE TABLE morphl.ga_chp_bq_predictions_access_logs (
  client_id text,
  tstamp timestamp,
  prediction double,
  PRIMARY KEY ((client_id), tstamp)
) WITH CLUSTERING ORDER BY (tstamp DESC);

CREATE TABLE morphl.ga_chp_bq_valid_models (
  always_zero int,
  day_as_str text,
  tstamp timestamp,
  unique_hash text,
  threshold double,
  accuracy double,
  loss double,
  is_model_valid boolean,
  PRIMARY KEY ((always_zero), day_as_str, tstamp, unique_hash)
) WITH CLUSTERING ORDER BY (day_as_str DESC, tstamp DESC);

CREATE TABLE morphl.ga_chp_bq_config_parameters (
  morphl_component_name text,
  parameter_name text,
  parameter_value text,
  PRIMARY KEY ((morphl_component_name, parameter_name))
);

INSERT INTO morphl.ga_chp_bq_config_parameters (morphl_component_name,parameter_name,parameter_value)
VALUES ('ga_chp_bq','days_training_interval','60');

INSERT INTO morphl.ga_chp_bq_config_parameters (morphl_component_name,parameter_name,parameter_value)
VALUES ('ga_chp_bq','days_prediction_interval','60');