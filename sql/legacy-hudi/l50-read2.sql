-- L50 读回：ods_vehicle_od2（batch）
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE ods_vehicle_od2 (
  PlateNumber   STRING,
  Origin        INT,
  OrgTimeStamp  STRING,
  Destination   INT,
  DesTimeStamp  STRING,
  PRIMARY KEY (PlateNumber, OrgTimeStamp) NOT ENFORCED
) WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://bigdata-live01-01:9000/warehouse/ods/vehicle_od2',
  'table.type' = 'MERGE_ON_READ'
);

SELECT * FROM ods_vehicle_od2;
