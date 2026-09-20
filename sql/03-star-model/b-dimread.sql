SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
CREATE CATALOG paimon WITH ('type'='paimon','warehouse'='hdfs://bigdata-live01-01:9000/warehouse/paimon');
USE CATALOG paimon;
SELECT * FROM dim_pub_taxi_base_info;
SELECT 'city' t, COUNT(*) n FROM dim_pub_city_info
UNION ALL SELECT 'taxi_type', COUNT(*) FROM dim_pub_taxi_type_info
UNION ALL SELECT 'company', COUNT(*) FROM dim_pub_taxi_company_info
UNION ALL SELECT 'taxi_base', COUNT(*) FROM dim_pub_taxi_base_info
UNION ALL SELECT 'date', COUNT(*) FROM dim_pub_date_info
UNION ALL SELECT 'time', COUNT(*) FROM dim_pub_time_info;
