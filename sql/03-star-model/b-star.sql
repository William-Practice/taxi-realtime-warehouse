-- 阶段B：星型模型（事实 tax i_gps_detail + 6 维表）DDL + 样例数据
USE traffic;

-- ============ 维度表 ============
DROP TABLE IF EXISTS dim_pub_date_info;
CREATE TABLE dim_pub_date_info (
  gps_date_key  VARCHAR(32) PRIMARY KEY,
  gps_year      VARCHAR(255),
  gps_year_ch   VARCHAR(255),
  gps_month     VARCHAR(255),
  gps_month_ch  VARCHAR(255),
  gps_day       VARCHAR(255),
  gps_day_ch    VARCHAR(255)
);

DROP TABLE IF EXISTS dim_pub_time_info;
CREATE TABLE dim_pub_time_info (
  gps_time_key  VARCHAR(32) PRIMARY KEY,
  gps_h         VARCHAR(255),
  gps_h_ch      VARCHAR(255),
  gps_m         VARCHAR(255),
  gps_m_ch      VARCHAR(255),
  gps_s         VARCHAR(255),
  gps_s_ch      VARCHAR(255)
);

DROP TABLE IF EXISTS dim_pub_city_info;
CREATE TABLE dim_pub_city_info (
  city_key      VARCHAR(32) PRIMARY KEY,
  city_id       VARCHAR(32),
  city_name     VARCHAR(255),
  city_status   VARCHAR(255)
);

DROP TABLE IF EXISTS dim_pub_taxi_type_info;
CREATE TABLE dim_pub_taxi_type_info (
  taxi_type_key VARCHAR(32) PRIMARY KEY,
  taxi_type_id  VARCHAR(32),
  taxi_type_name VARCHAR(255)
);

DROP TABLE IF EXISTS dim_pub_taxi_company_info;
CREATE TABLE dim_pub_taxi_company_info (
  taxi_company_key          VARCHAR(32) PRIMARY KEY,
  taxi_company_id           INT,
  taxi_company_name         VARCHAR(255),
  taxi_company_addr         VARCHAR(255),
  taxi_company_introduction VARCHAR(512),
  taxi_company_complaints   VARCHAR(255),
  taxi_company_serviceCall  VARCHAR(64)
);

DROP TABLE IF EXISTS dim_pub_taxi_base_info;
CREATE TABLE dim_pub_taxi_base_info (
  taxi_key          VARCHAR(32) PRIMARY KEY,
  taxi_number       VARCHAR(32),
  taxi_color        VARCHAR(255),
  taxi_brand        VARCHAR(255),
  taxi_type         VARCHAR(255),
  taxi_company_key  VARCHAR(32)
);

-- ============ 事实表 ============
DROP TABLE IF EXISTS taxi_gps_detail;
CREATE TABLE taxi_gps_detail (
  taxi_key          VARCHAR(255),
  gps_date_key      DATETIME,
  gps_time_key      DATETIME,
  city_key          VARCHAR(255),
  taxi_company_key  VARCHAR(255),
  taxi_type_key     VARCHAR(255),
  taxi_longitude    VARCHAR(255),
  taxi_latitude     VARCHAR(255),
  taxi_speed        DOUBLE,
  taxi_direction    VARCHAR(255),
  taxi_status       VARCHAR(255)
);

-- ============ 样例数据 ============
INSERT INTO dim_pub_city_info VALUES
 ('CITY001','1','上海','1'),
 ('CITY002','2','北京','1');

INSERT INTO dim_pub_taxi_type_info VALUES
 ('TY001','1','小型轿车'),
 ('TY002','2','SUV');

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

INSERT INTO taxi_gps_detail VALUES
 ('TX001','2026-09-20 13:05:00','2026-09-20 13:05:00','CITY001','C002','TY001','121.470000','31.230000',45.5,'东北','载客'),
 ('TX001','2026-09-20 12:00:00','2026-09-20 12:00:00','CITY001','C002','TY001','121.480000','31.240000',0.0,'北','空车'),
 ('TX002','2026-09-20 13:05:00','2026-09-20 13:05:00','CITY002','C001','TY001','116.400000','39.900000',62.3,'西南','载客');

-- 验证
SELECT 'date' t, COUNT(*) n FROM dim_pub_date_info
UNION ALL SELECT 'time', COUNT(*) FROM dim_pub_time_info
UNION ALL SELECT 'city', COUNT(*) FROM dim_pub_city_info
UNION ALL SELECT 'taxi_type', COUNT(*) FROM dim_pub_taxi_type_info
UNION ALL SELECT 'company', COUNT(*) FROM dim_pub_taxi_company_info
UNION ALL SELECT 'taxi_base', COUNT(*) FROM dim_pub_taxi_base_info
UNION ALL SELECT 'fact_gps', COUNT(*) FROM taxi_gps_detail;
