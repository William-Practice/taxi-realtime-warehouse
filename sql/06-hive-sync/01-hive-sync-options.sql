-- Paimon → Hive 同步（hive_sync 到 Hive metastore）
SET 'execution.runtime-mode' = 'batch';
SET 'execution.batch.adaptive.auto-parallelism.enabled' = 'false';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);
USE CATALOG paimon;

CREATE TABLE IF NOT EXISTS sync_demo (
  id   INT,
  name STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'bucket' = '1',
  'hive_sync.enabled' = 'true',
  'hive_sync.mode' = 'hms',
  'hive_sync.metastore.uris' = 'thrift://bigdata-live01-01:9083',
  'hive_sync.db' = 'traffic',
  'hive_sync.table' = 'sync_demo'
);

INSERT INTO sync_demo VALUES (1, 'alpha'), (2, 'beta');
