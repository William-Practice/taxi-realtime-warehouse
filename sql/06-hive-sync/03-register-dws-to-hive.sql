-- 把真实项目表注册进 Hive：dws_city_traffic → Hive-backed Paimon catalog
SET 'execution.runtime-mode' = 'batch';
SET 'execution.batch.adaptive.auto-parallelism.enabled' = 'false';

-- 文件系统 catalog（有现成的 dws）
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);
-- Hive-backed catalog（表注册进 Hive metastore）
CREATE CATALOG paimon_hive WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://bigdata-live01-01:9083',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon_hive'
);

CREATE TABLE paimon_hive.`default`.dws_city_traffic (
  city_name  STRING,
  dt         STRING,
  gps_points BIGINT,
  taxi_cnt   BIGINT,
  avg_speed  DOUBLE,
  load_cnt   BIGINT,
  PRIMARY KEY (city_name, dt) NOT ENFORCED
) WITH ('bucket' = '1', 'sink.parallelism' = '1');

INSERT INTO paimon_hive.`default`.dws_city_traffic
SELECT city_name, dt, gps_points, taxi_cnt, avg_speed, load_cnt
FROM paimon.`default`.dws_city_traffic;
