-- 阶段B：6 张维表 → Paimon DIM（batch 初始化）
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG paimon WITH (
  'type' = 'paimon',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon'
);
USE CATALOG paimon;

CREATE TABLE if not exists dim_pub_city_info (
  city_key STRING, city_id STRING, city_name STRING, city_status STRING,
  PRIMARY KEY (city_key) NOT ENFORCED) WITH ('bucket'='1');
CREATE TABLE if not exists dim_pub_taxi_type_info (
  taxi_type_key STRING, taxi_type_id STRING, taxi_type_name STRING,
  PRIMARY KEY (taxi_type_key) NOT ENFORCED) WITH ('bucket'='1');
CREATE TABLE if not exists dim_pub_taxi_company_info (
  taxi_company_key STRING, taxi_company_id INT, taxi_company_name STRING,
  taxi_company_addr STRING, taxi_company_introduction STRING,
  taxi_company_complaints STRING, taxi_company_serviceCall STRING,
  PRIMARY KEY (taxi_company_key) NOT ENFORCED) WITH ('bucket'='1');
CREATE TABLE if not exists dim_pub_taxi_base_info (
  taxi_key STRING, taxi_number STRING, taxi_color STRING, taxi_brand STRING,
  taxi_type STRING, taxi_company_key STRING,
  PRIMARY KEY (taxi_key) NOT ENFORCED) WITH ('bucket'='1');
CREATE TABLE if not exists dim_pub_date_info (
  gps_date_key STRING, gps_year STRING, gps_year_ch STRING, gps_month STRING,
  gps_month_ch STRING, gps_day STRING, gps_day_ch STRING,
  PRIMARY KEY (gps_date_key) NOT ENFORCED) WITH ('bucket'='1');
CREATE TABLE if not exists dim_pub_time_info (
  gps_time_key STRING, gps_h STRING, gps_h_ch STRING, gps_m STRING,
  gps_m_ch STRING, gps_s STRING, gps_s_ch STRING,
  PRIMARY KEY (gps_time_key) NOT ENFORCED) WITH ('bucket'='1');

INSERT INTO dim_pub_city_info VALUES
 ('CITY001','1','上海','1'),('CITY002','2','北京','1');
INSERT INTO dim_pub_taxi_type_info VALUES
 ('TY001','1','小型轿车'),('TY002','2','SUV');
INSERT INTO dim_pub_taxi_company_info VALUES
 ('C001',1,'强生出租','上海市徐汇区','上海强生出租汽车有限公司','无','400-820-8888'),
 ('C002',2,'大众出租','上海市静安区','大众交通集团','无','021-96822');
INSERT INTO dim_pub_taxi_base_info VALUES
 ('TX001','沪A55555','蓝','大众','TY001','C002'),
 ('TX002','沪B12345','黄','强生','TY001','C001');
INSERT INTO dim_pub_date_info VALUES
 ('20260920','2026','二〇二六','09','九月','20','二十'),
 ('20260921','2026','二〇二六','09','九月','21','二十一');
INSERT INTO dim_pub_time_info VALUES
 ('20260920130500','13','十三时','05','零五分','00','零零秒'),
 ('20260920120000','12','十二时','00','零零分','00','零零秒');
