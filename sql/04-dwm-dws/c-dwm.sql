-- 阶段C：DWM 宽表 + DWS 聚合（Paimon，batch）
SET 'execution.runtime-mode' = 'batch';
SET 'execution.batch.adaptive.auto-parallelism.enabled' = 'false';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);
USE CATALOG paimon;

-- ===== DWM：事实 JOIN 6 维，拉宽 =====
CREATE TABLE IF NOT EXISTS dwm_taxi_gps_wide (
  taxi_key STRING, taxi_number STRING, taxi_color STRING, taxi_brand STRING,
  gps_date_key TIMESTAMP(3), gps_time_key TIMESTAMP(3),
  city_key STRING, city_name STRING,
  taxi_company_key STRING, taxi_company_name STRING,
  taxi_type_key STRING, taxi_type_name STRING,
  taxi_longitude STRING, taxi_latitude STRING,
  taxi_speed DOUBLE, taxi_direction STRING, taxi_status STRING,
  PRIMARY KEY (taxi_key, gps_time_key) NOT ENFORCED
) WITH ('bucket' = '1', 'sink.parallelism' = '1');

INSERT INTO dwm_taxi_gps_wide
SELECT o.taxi_key, tb.taxi_number, tb.taxi_color, tb.taxi_brand,
       o.gps_date_key, o.gps_time_key,
       o.city_key, c.city_name,
       o.taxi_company_key, co.taxi_company_name,
       o.taxi_type_key, ty.taxi_type_name,
       o.taxi_longitude, o.taxi_latitude,
       o.taxi_speed, o.taxi_direction, o.taxi_status
FROM ods_taxi_gps_detail o
LEFT JOIN dim_pub_taxi_base_info  tb ON o.taxi_key = tb.taxi_key
LEFT JOIN dim_pub_city_info       c  ON o.city_key = c.city_key
LEFT JOIN dim_pub_taxi_company_info co ON o.taxi_company_key = co.taxi_company_key
LEFT JOIN dim_pub_taxi_type_info  ty ON o.taxi_type_key = ty.taxi_type_key;

-- ===== DWS：按城市+日期聚合指标 =====
CREATE TABLE IF NOT EXISTS dws_city_traffic (
  city_name STRING, dt STRING,
  gps_points BIGINT,           -- GPS 点数
  taxi_cnt   BIGINT,           -- 车辆数
  avg_speed  DOUBLE,           -- 平均车速
  load_cnt   BIGINT,           -- 载客次数
  PRIMARY KEY (city_name, dt) NOT ENFORCED
) WITH ('bucket' = '1', 'sink.parallelism' = '1');

INSERT INTO dws_city_traffic
SELECT city_name,
       DATE_FORMAT(gps_date_key, 'yyyyMMdd') AS dt,
       COUNT(*) AS gps_points,
       COUNT(DISTINCT taxi_key) AS taxi_cnt,
       AVG(taxi_speed) AS avg_speed,
       SUM(CASE WHEN taxi_status = '载客' THEN 1 ELSE 0 END) AS load_cnt
FROM dwm_taxi_gps_wide
GROUP BY city_name, DATE_FORMAT(gps_date_key, 'yyyyMMdd');
