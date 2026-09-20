-- 阶段D：ADS serving 表（MySQL）
CREATE TABLE IF NOT EXISTS traffic.ads_city_realtime (
  city_name   VARCHAR(64),
  dt          VARCHAR(16),
  gps_points  BIGINT,
  taxi_cnt    BIGINT,
  avg_speed   DOUBLE,
  load_cnt    BIGINT,
  update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (city_name, dt)
);
-- 给 JDBC sink 写权限
GRANT INSERT, UPDATE, DELETE ON traffic.* TO 'cdc'@'%';
FLUSH PRIVILEGES;
SHOW COLUMNS FROM traffic.ads_city_realtime;
