-- Paimon Catalog 直接挂 Hive metastore（表自动注册进 Hive）
SET 'execution.runtime-mode' = 'batch';
SET 'execution.batch.adaptive.auto-parallelism.enabled' = 'false';

CREATE CATALOG paimon_hive WITH (
  'type' = 'paimon',
  'metastore' = 'hive',
  'uri' = 'thrift://bigdata-live01-01:9083',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);
USE CATALOG paimon_hive;

CREATE TABLE IF NOT EXISTS hive_demo (
  id   INT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket' = '1'
);

INSERT INTO hive_demo VALUES (1, 'x'), (2, 'y');
