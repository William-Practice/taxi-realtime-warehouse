-- 阶段B：事实表 taxi_gps_detail → Paimon ODS（流式 CDC）
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.min-pause' = '15 s';
SET 'execution.checkpointing.timeout' = '10 min';

CREATE TABLE mysql_taxi_gps_detail (
  taxi_key          STRING,
  gps_date_key      TIMESTAMP(3),
  gps_time_key      TIMESTAMP(3),
  city_key          STRING,
  taxi_company_key  STRING,
  taxi_type_key     STRING,
  taxi_longitude    STRING,
  taxi_latitude     STRING,
  taxi_speed        DOUBLE,
  taxi_direction    STRING,
  taxi_status       STRING,
  PRIMARY KEY (taxi_key, gps_time_key) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname'  = 'localhost',
  'port'      = '3306',
  'username'  = 'cdc',
  'password'  = 'CDC_PASSWORD',
  'database-name' = 'traffic',
  'table-name'    = 'taxi_gps_detail',
  'scan.incremental.snapshot.enabled' = 'false',
  'server-time-zone' = 'UTC'
);

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);
USE CATALOG paimon;

CREATE TABLE ods_taxi_gps_detail (
  taxi_key          STRING,
  gps_date_key      TIMESTAMP(3),
  gps_time_key      TIMESTAMP(3),
  city_key          STRING,
  taxi_company_key  STRING,
  taxi_type_key     STRING,
  taxi_longitude    STRING,
  taxi_latitude     STRING,
  taxi_speed        DOUBLE,
  taxi_direction    STRING,
  taxi_status       STRING,
  PRIMARY KEY (taxi_key, gps_time_key) NOT ENFORCED
) WITH (
  'bucket' = '1'
);

INSERT INTO ods_taxi_gps_detail
SELECT * FROM default_catalog.default_database.mysql_taxi_gps_detail;
