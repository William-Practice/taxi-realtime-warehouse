-- 阶段D+：真·实时 —— MySQL CDC(事实流) → Paimon DIM lookup join → 流式聚合 → 持续写 MySQL ADS
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';
SET 'execution.checkpointing.min-pause' = '5 s';
SET 'execution.checkpointing.timeout' = '10 min';

-- Paimon catalog（维表在其下）
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);

-- 事实源：MySQL CDC（流，带处理时间用于 lookup join）
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
  proc_time         AS PROCTIME()
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

-- ADS 目标（MySQL，JDBC upsert）
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
  'password' = 'CDC_PASSWORD',
  'driver' = 'com.mysql.cj.jdbc.Driver'
);

-- 流式聚合 + 维表 lookup join（Paimon DIM）
INSERT INTO ads_city_realtime
SELECT c.city_name,
       DATE_FORMAT(o.gps_date_key, 'yyyyMMdd') AS dt,
       COUNT(*)                                 AS gps_points,
       COUNT(DISTINCT o.taxi_key)               AS taxi_cnt,
       AVG(o.taxi_speed)                        AS avg_speed,
       SUM(CASE WHEN o.taxi_status = '载客' THEN 1 ELSE 0 END) AS load_cnt
FROM mysql_taxi_gps_detail AS o
LEFT JOIN `paimon`.`default`.`dim_pub_city_info`
     FOR SYSTEM_TIME AS OF o.proc_time AS c
     ON o.city_key = c.city_key
GROUP BY c.city_name, DATE_FORMAT(o.gps_date_key, 'yyyyMMdd');
