-- L49：Hudi 写入 HDFS 隔离测试（batch 模式，写完即退出）
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE ods_vehicle_od (
  PlateNumber   STRING,
  Origin        INT,
  OrgTimeStamp  STRING,
  Destination   INT,
  DesTimeStamp  STRING,
  PRIMARY KEY (PlateNumber, OrgTimeStamp) NOT ENFORCED
) WITH (
  'connector' = 'hudi',
  'path' = 'hdfs://bigdata-live01-01:9000/warehouse/ods/vehicle_od',
  'table.type' = 'MERGE_ON_READ',
  'write.precombine.field' = 'OrgTimeStamp'
);

INSERT INTO ods_vehicle_od
VALUES ('京Z00001', 9, '2026-09-20 10:00:00', 10, '2026-09-20 10:30:00');
