-- 阶段D：Paimon DWS → MySQL ADS（JDBC sink）
SET 'execution.runtime-mode' = 'batch';
SET 'execution.batch.adaptive.auto-parallelism.enabled' = 'false';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);

CREATE TABLE ads_city_realtime (
  city_name  STRING,
  dt         STRING,
  gps_points BIGINT,
  taxi_cnt   BIGINT,
  avg_speed  DOUBLE,
  load_cnt   BIGINT,
  PRIMARY KEY (city_name, dt) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://localhost:3306/traffic',
  'table-name' = 'ads_city_realtime',
  'username' = 'cdc',
  'password' = 'Cdc@123456',
  'driver' = 'com.mysql.cj.jdbc.Driver'
);

INSERT INTO ads_city_realtime
SELECT city_name, dt, gps_points, taxi_cnt, avg_speed, load_cnt
FROM `paimon`.`default`.`dws_city_traffic`;
