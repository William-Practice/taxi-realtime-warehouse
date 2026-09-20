SET 'execution.runtime-mode' = 'batch';
SET 'execution.batch.adaptive.auto-parallelism.enabled' = 'false';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
CREATE CATALOG paimon WITH ('type'='paimon','warehouse'='hdfs://bigdata-live01-01:9000/warehouse/paimon');
USE CATALOG paimon;
SELECT taxi_number, city_name, taxi_company_name, taxi_type_name, taxi_speed, taxi_status, gps_time_key FROM dwm_taxi_gps_wide;
SELECT * FROM dws_city_traffic;
