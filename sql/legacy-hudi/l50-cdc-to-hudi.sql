-- L50：MySQL CDC → Hudi ODS 落湖
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '10 s';

-- 源：MySQL traffic.vehicle_od（表无主键 → 关掉增量快照，用 legacy 快照）
CREATE TABLE mysql_vehicle_od (
  PlateNumber   STRING,
  Origin        INT,
  OrgTimeStamp  STRING,
  Destination   INT,
  DesTimeStamp  STRING,
  PRIMARY KEY (PlateNumber, OrgTimeStamp) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname'  = 'localhost',
  'port'      = '3306',
  'username'  = 'cdc',
  'password'  = 'CDC_PASSWORD',
  'database-name' = 'traffic',
  'table-name'    = 'vehicle_od',
  'scan.incremental.snapshot.enabled' = 'false',
  'server-time-zone' = 'UTC'
);

-- 目标：Hudi ODS 湖表
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

INSERT INTO ods_vehicle_od SELECT * FROM mysql_vehicle_od;
