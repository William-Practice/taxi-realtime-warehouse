-- L47：交通 OD 数仓 —— 源表（对应课程 表6.62~6.65）
-- 库名用 traffic（如课程用别的库名，改这里即可）
CREATE DATABASE IF NOT EXISTS traffic DEFAULT CHARSET utf8mb4;
USE traffic;

-- 表 6.62 车辆 OD 数据
CREATE TABLE IF NOT EXISTS vehicle_od (
  PlateNumber   VARCHAR(16)  COMMENT '车牌号',
  Origin        INT          COMMENT '起始交通小区编号',
  OrgTimeStamp  VARCHAR(32)  COMMENT '起始时间戳',
  Destination   INT          COMMENT '到达交通小区编号',
  DesTimeStamp  VARCHAR(32)  COMMENT '到达时间戳'
);

-- 表 6.63 车辆实时位置数据
CREATE TABLE IF NOT EXISTS vehicle_realtime (
  DeviceId      VARCHAR(32)  COMMENT '监控点ID',
  PlateNumber   VARCHAR(16)  COMMENT '车牌号',
  VehicleColor  VARCHAR(16)  COMMENT '车辆颜色',
  RecordTime    VARCHAR(32)  COMMENT '记录时间'
);

-- 表 6.64 识别追逐车辆
CREATE TABLE IF NOT EXISTS chase_vehicle (
  TrackID       VARCHAR(32)  COMMENT '追逐编号',
  MonitorID     VARCHAR(32)  COMMENT '设备监控ID',
  Location      VARCHAR(64)  COMMENT '设备监控位置',
  Longitude     DOUBLE       COMMENT '经度',
  Latitude      DOUBLE       COMMENT '纬度',
  TimeStamp     VARCHAR(32)  COMMENT '识别时间戳'
);

-- 表 6.65 违章车辆数据
CREATE TABLE IF NOT EXISTS violation_vehicle (
  PlateNumber   VARCHAR(16)  COMMENT '车牌号',
  MonitorID     VARCHAR(32)  COMMENT '设备监控ID',
  Location      VARCHAR(64)  COMMENT '设备监控位置',
  Type          VARCHAR(32)  COMMENT '违章类型',
  TimeStamp     VARCHAR(32)  COMMENT '违章时间',
  PictureID     VARCHAR(32)  COMMENT '违章拍照编号'
);

-- ============ 样例数据 ============
INSERT INTO vehicle_od VALUES
 ('京A12345', 1, '2026-09-20 08:00:00', 2, '2026-09-20 08:25:00'),
 ('京B67890', 3, '2026-09-20 08:10:00', 5, '2026-09-20 08:40:00'),
 ('京C11111', 2, '2026-09-20 09:00:00', 4, '2026-09-20 09:30:00');

INSERT INTO vehicle_realtime VALUES
 ('DEV001', '京A12345', '黑', '2026-09-20 08:05:00'),
 ('DEV002', '京B67890', '白', '2026-09-20 08:15:00'),
 ('DEV003', '京C11111', '蓝', '2026-09-20 09:05:00');

INSERT INTO chase_vehicle VALUES
 ('T001', 'MON01', '路口A', 116.40, 39.90, '2026-09-20 08:06:00'),
 ('T002', 'MON02', '路口B', 116.41, 39.91, '2026-09-20 08:16:00');

INSERT INTO violation_vehicle VALUES
 ('京A12345', 'MON01', '路口A', '超速', '2026-09-20 08:07:00', 'PIC001'),
 ('京B67890', 'MON02', '路口B', '闯红灯', '2026-09-20 08:17:00', 'PIC002');

-- ============ 验证 ============
SHOW TABLES;
SELECT 'vehicle_od' t, COUNT(*) n FROM vehicle_od
UNION ALL SELECT 'vehicle_realtime', COUNT(*) FROM vehicle_realtime
UNION ALL SELECT 'chase_vehicle', COUNT(*) FROM chase_vehicle
UNION ALL SELECT 'violation_vehicle', COUNT(*) FROM violation_vehicle;
