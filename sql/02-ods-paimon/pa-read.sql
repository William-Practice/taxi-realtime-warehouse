-- 阶段A 读回：Paimon ODS（batch）
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);
USE CATALOG paimon;

SELECT * FROM ods_vehicle_od;
