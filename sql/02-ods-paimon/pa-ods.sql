-- 阶段A：MySQL CDC → Paimon ODS（用 Paimon Catalog）
SET 'execution.runtime-mode' = 'streaming';
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.min-pause' = '15 s';
SET 'execution.checkpointing.timeout' = '10 min';

-- 源：MySQL traffic.vehicle_od（default catalog）
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

-- Paimon Catalog
CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);
USE CATALOG paimon;

-- Paimon 湖表（主键表，upsert）
CREATE TABLE ods_vehicle_od (
  PlateNumber   STRING,
  Origin        INT,
  OrgTimeStamp  STRING,
  Destination   INT,
  DesTimeStamp  STRING,
  PRIMARY KEY (PlateNumber, OrgTimeStamp) NOT ENFORCED
) WITH (
  'bucket' = '1'
);

INSERT INTO ods_vehicle_od
SELECT * FROM default_catalog.default_database.mysql_vehicle_od;
