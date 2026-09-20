-- L50 净测：CDC → Hudi(全新路径 ods_vehicle_od2)，验证 commit 能真正完成
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.min-pause' = '15 s';
SET 'execution.checkpointing.timeout' = '10 min';

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
  'password'  = 'Cdc@123456',
  'database-name' = 'traffic',
  'table-name'    = 'vehicle_od',
  'scan.incremental.snapshot.enabled' = 'false',
  'server-time-zone' = 'UTC'
);

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
  'table.type' = 'MERGE_ON_READ',
  'write.precombine.field' = 'OrgTimeStamp'
);

INSERT INTO ods_vehicle_od2 SELECT * FROM mysql_vehicle_od;
