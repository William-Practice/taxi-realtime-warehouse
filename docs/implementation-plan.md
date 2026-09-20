# L47~L62 实时数仓项目实施计划

> **执行环境**：虚机 `bigdata-live01-01`（`192.168.56.101`，用户 `wry`）。**除特别标注外，所有命令都在虚机里执行**（先 `ssh wry@192.168.56.101`，提示符变成 `wry@bigdata-live01-01` 才是虚机）。
> **宿主机**只做两件事：ssh/scp 进出文件。

**目标**：用 Flink CDC + Hudi + Phoenix + Redis + ClickHouse + Flume + WebSocket，搭一条可跑通、可写进简历的实时数仓链路。

**架构**：

```
MySQL binlog → Flink CDC → ODS(Hudi) ──→ DIM(Phoenix) ──→ DWM(Phoenix+Redis) ──→ DWS(ClickHouse) ──→ ADS(Flume/WS → 前端)
```

**技术栈**：Flink 1.17.2 (Scala 2.12) · Kafka 3.2.0 · Hudi 0.14.1 · Flink CDC 2.4.2 · HBase 2.3.2 + Phoenix 5.1.2 · Redis · ClickHouse · Flume 1.11.0 · MySQL · Hadoop 3.2.0。

---

## 现状盘点（2026-09-20）

| 项 | 状态 |
|----|------|
| Java 8 / Hadoop 3.2.0 / Hive 3.1.0 / ZK 3.8.4 / Kafka 3.2.0 / HBase 2.3.2 / Spark 3.5.0 / Flink 1.17.2 | ✅ 已装 |
| MySQL / Redis | ✅ 已装（当前仅这两个在运行） |
| **Hudi / Flink CDC 连接器** | ❌ 未装（L47/L48 前置） |
| **Phoenix** | ❌ 未装（L56） |
| **ClickHouse** | ❌ 未装（L59） |
| **Flume** | ❌ 未装（L61） |
| **Maven** | ❌ 未装（L48"Hudi 编译"需要） |
| 基础栈运行状态 | ⛔ 全停（ZK/Kafka/Hadoop/Hive/HBase/Flink 都没跑） |

> ⚠️ 本计划是"标准链路"版本，与课程视频的具体操作可能有个别出入。**每完成一节，请对照视频校准**；发现偏差告诉我，我改计划。

### MySQL 账号（已确认）

| 用户名 | 密码 | 权限 | 用途 |
|--------|------|------|------|
| `hive_user` | `HIVE_PASSWORD` | `%`、`bigdata-live01-01` | Hive metastore |
| `sqoop` | `SQOOP_PASSWORD` | `%`、`192.168.56.%`、`localhost` | Sqoop 导入导出 |
| `root` | （有密码，localhost） | `localhost` | 管理；`sudo mysql` 可 socket 免密 |
| `debian-sys-maint` | 系统维护 | `localhost` | Debian 包管理 |

> Flink CDC 需要一个具备 `REPLICATION SLAVE / REPLICATION CLIENT` 权限的账号——计划里新建了 `cdc / CDC_PASSWORD`（见 Task 1.1）。
>
> ⚠️ 上表里的密码均为**占位符**（仓库不保存真实凭据）。本地实验环境要用时，一行命令替换成你自己的密码：
>
> ```bash
> find sql -name '*.sql' -exec sed -i "s/CDC_PASSWORD/<你的密码>/g" {} +
> ```
>
> 其余两个账号同理（`HIVE_PASSWORD` / `SQOOP_PASSWORD`）。
> `scripts/ads_ws.py` 已改为从环境变量读取，不需要替换。

### 进展记录

- **2026-09-20 阶段〇 完成**：ZK / Kafka / HDFS+YARN / HBase / Hive Metastore(tmux `hive`) / Flink(UI:8083) / MySQL / Redis 全部启动。一键脚本：`/home/wry/dc-stack.sh {start|stop|restart|status}`。
- **2026-09-20 L47 完成**：`shop` 库 + `orders` 表 + `cdc` 账号就绪（binlog 本就 ON/ROW/FULL）；CDC 连接器 `2.4.2`、Hudi bundle `0.14.1` 已放入 `/opt/flink-1.17.2/lib`；Flink CDC→print 验证通过（7 行，含实时新增行）。
  - 踩坑①：非交互 sql-client 只能用 `SET 'sql-client.execution.result-mode'='TABLEAU'`。
  - 踩坑②：`server-time-zone` 必须与 MySQL 服务器一致，本机为 **UTC**（否则报 timezone offset 不匹配）。
  - 踩坑③：`SELECT` 结果会回传客户端并被缓冲；验证请用 `connector='print'`（结果写到 TaskManager stdout，可 grep）。
- **2026-09-20 L48 完成**：Hudi bundle `0.14.1` + `flink-shaded-hadoop-2-uber-2.8.3-10.0` 加入 `/opt/flink-1.17.2/lib`，重启生效。
- **2026-09-20 L49 完成**：建 Hudi MOR 湖表，写入 HDFS 成功（`.hoodie` + deltacommit + log 数据文件）。
- **2026-09-20 L50 完成**：MySQL CDC → Hudi ODS 打通并稳定（checkpoint 全成功、commit 连续完成、表可读回 7 行含实时新增）。
  - 踩坑④：Hudi 的 path 必须写全 `hdfs://bigdata-live01-01:9000/...`，写 `localhost:9000` 会 Connection refused（NameNode 绑在 192.168.56.101）。
  - 踩坑⑤：Flink 默认 `taskmanager.numberOfTaskSlots: 1` 不够，多算子/多作业会 `NoResourceAvailableException`；改为 4。
  - 踩坑⑥：Hudi MOR 提交慢，checkpoint 间隔太小（10s）会 checkpoint declined；放大到 30s 并设 min-pause/timeout。
  - 踩坑⑦【关键根因】：HDFS 写入因 **`dfs.replication=2` 且从节点 DataNode(192.168.56.102) 磁盘故障** → `Failed to replace a bad datanode ... pipeline` → Hudi append 失败 → checkpoint 反复 declined → 作业无限重启。修复：`hdfs-site.xml` 改 `dfs.replication=1`（只写本地 DN），并让 Flink 读取 hadoop 配置 `env.hadoop.conf.dir: /opt/hadoop/etc/hadoop`（改 hdfs-site.xml 需 sudo）。
  - 验证方式：`curl localhost:8083/jobs/<jid>/checkpoints`（看 completed/failed/restored）+ `hdfs dfs -ls <path>/.hoodie | grep deltacommit`。

## 现代栈改版（2026-09-20）

原课程栈偏老，决定**保留数仓骨架、换现代栈**：

| 层 | 老课程栈 | 现代栈（本项目） |
|----|---------|-----------------|
| CDC | Flink CDC 2.4.2 + Flink 1.17 | 保留 Flink 1.17.2 + CDC 2.4.2（够用） |
| 湖/ODS | Hudi 0.14 | **Paimon 0.8.0**（Flink 1.17 原生兼容，已装入 lib） |
| DIM | Phoenix + HBase | Paimon 维表 + Redis 热维 |
| DWM/DWS | Phoenix/ClickHouse | **Paimon**（建表 + 聚合查询） |
| OLAP | ClickHouse / Doris | 受 VM 内存限制（仅 3.8G），暂用 **Paimon 内嵌查询**；如需独立 OLAP 另议 |
| ADS | Flume + WebSocket | Paimon/Doris 结果 → 轻量 WebSocket/API |

**资源现状**：VM 总内存 3.8G；已停 HBase/YARN/Spark 腾内存（可用 ~984M）。
保留服务：HDFS(NameNode/DataNode/SecondaryNN) + ZK + Kafka + Flink(JM/TM) + Hive Metastore。

**修订路线**：
- 阶段A：**ODS on Paimon**（CDC → Paimon 湖表）替换 Hudi ODS —— ✅ 2026-09-20 完成
  - Paimon 表：`/warehouse/paimon/default.db/ods_vehicle_od`（主键表，bucket=1，ORC）
  - 结果：CDC 快照+实时提交（snapshot 连续），读回 8 行
  - 踩坑：Paimon 直接用 `'path'` 建表会报 `Schema file not found`，**必须走 Paimon Catalog**（`CREATE CATALOG ... 'type'='paimon'` + `USE CATALOG`），且 INSERT 跨 catalog 源表要写全限定名
- 阶段B：星型模型（事实 `taxi_gps_detail` + 6 维）落地 —— ✅ 2026-09-20 完成
  - MySQL `traffic` 库：事实表 `taxi_gps_detail` + 6 张 `dim_pub_*`（含样例数据）
  - 事实表 CDC → Paimon ODS 表 `ods_taxi_gps_detail`（流式，snapshot 连续）
  - 6 维表 → Paimon DIM（batch 初始化，各 2 行）
- 阶段C：DIM/DWM/DWS（Paimon 宽表 + 聚合）—— ✅ 2026-09-20 完成
  - `dwm_taxi_gps_wide`：事实 LEFT JOIN 6 维拉宽（4 行）
  - `dws_city_traffic`：按城市+日期聚合（GPS点数/车辆数/平均车速/载客次数）
  - 踩坑：Paimon Sink **不支持 Flink batch 自适应并行度** → 必须 `SET 'execution.batch.adaptive.auto-parallelism.enabled'='false'`（只设 `sink.parallelism` 无效）
- 阶段D：ADS + WebSocket —— ✅ 2026-09-20 完成
  - ADS：Paimon DWS → **MySQL `ads_city_realtime`**（Flink JDBC sink）
  - WS 服务：`/home/wry/ads_ws.py`（pymysql 轮询 ADS，每 2s 推 JSON），监听 **8765**
  - 大屏：`/home/wry/web/ads_dashboard.html`，用 `python3 -m http.server 8080` 托管
  - **查看**：浏览器打开 `http://192.168.56.101:8080/ads_dashboard.html`
  - 踩坑：Flink SQL 里 `default` 是保留字，跨 catalog 引用要加反引号 `` `paimon`.`default`.`dws_city_traffic` ``；JDBC sink 写用户需有 INSERT 权限
- 阶段D+（真·实时）—— ✅ 2026-09-20 完成：ADS 改**流式**，大屏自动刷新（~10s）
  - 链路：`MySQL CDC(事实流, 带 PROCTIME) → Paimon DIM lookup join (FOR SYSTEM_TIME AS OF) → 流式聚合 → 持续 UPSERT MySQL ads_city_realtime`
  - 实测：插入 1 条 GPS → 上海点数 3→4、载客 2→3 自动更新
  - 说明：因给已存在的 Paimon 表加 `PROCTIME()` 需重建源表，事实流直接用 mysql-cdc（仍是实时）；Paimon 仍作为 DIM/湖存储

- 阶段E：Paimon → Hive 同步 —— ✅ 2026-09-20 完成
  - Hive-backed Paimon catalog（`'metastore'='hive'`）把表注册进 Hive metastore
  - Flink lib 加 `flink-sql-connector-hive-2.3.9_2.12-1.17.2.jar`；Hive `auxlib/` 放 `paimon-hive-connector-3.1-0.8.0.jar` + `paimon-hive-catalog-0.8.0.jar`
  - 验证：`hive -e "select * from dws_city_traffic"` 返回上海/北京两行
  - 踩坑：`hive_sync` 表属性方式不生效（Hive 里查不到），改用 Hive-backed catalog；Hive 侧缺 Paimon 类 → 需 auxlib jar

> 原 Hudi 相关阶段（阶段二/三）保留在下方仅供对照，实操以 Paimon 为准。

### 项目领域校正（据 L47~L48 网盘视频）

真实项目 = **出租车/交通 GPS 的 OD（Origin-Destination 起讫点）实时数仓**，不是电商订单。

自动转写名词对照：`my stacle`=MySQL、`blog`/`circle belong`=binlog、`蝴蝶兰`/`Butterfly`=Hudi、`clean house`=ClickHouse、`Aspect`=Hive、`心形模型`=星型模型、`taxi`=出租车。

数据模型（星型）：
- **事实表**：GPS 轨迹数据 + 实时核心数据（车牌号、交通小区编号、起始时间、经纬度、速度…）
- **维度表**（维表放 master/MySQL）：日期、城市、业务类型、公司类型、车型、颜色

链路：`MySQL(binlog) → Flink CDC → ODS(Hudi) → Hive(血缘/分层) → ClickHouse(实时分析)`；常见指标：车速、车距、流量。

对照已完成项：binlog 已 ON（视频要手动开，我们跳过）；Flink CDC 已打通（✅）。

> 注：`shop.orders` 只是我 L47 用来说明 CDC 的占位表。正式 ODS 按课程 表6.62~6.65 的交通 OD 模型建表（见下）。

### 源表（课程 表6.62~6.65，ODS 来源）

库 `traffic`，4 张业务源表：

| 表 | 说明 | 字段 |
|----|------|------|
| `vehicle_od` (6.62) | 车辆 OD 数据 | PlateNumber(车牌号) · Origin(起始小区编号) · OrgTimeStamp(起始时间戳) · Destination(到达小区编号) · DesTimeStamp(到达时间戳) |
| `vehicle_realtime` (6.63) | 车辆实时位置 | DeviceId(监控点ID) · PlateNumber · VehicleColor(车辆颜色) · RecordTime(记录时间) |
| `chase_vehicle` (6.64) | 识别追逐车辆 | TrackID · MonitorID · Location · Longitude · Latitude · TimeStamp |
| `violation_vehicle` (6.65) | 违章车辆 | PlateNumber · MonitorID · Location · Type(违章类型) · TimeStamp · PictureID(拍照编号) |

> 时间字段课程统一用 `String`（非 DATETIME），建表照此。

### 数仓模型（星型，据课程模型图）

**事实表 `taxi_gps_detail`（出租车 GPS 明细）**

| 字段 | 类型 | 说明 |
|------|------|------|
| taxi_key | varchar | 出租车代理键(外键→dim_pub_taxi_base_info) |
| gps_date_key | datetime | GPS 日期键(外键→dim_pub_date_info) |
| gps_time_key | datetime | GPS 时间键(外键→dim_pub_time_info) |
| city_key | varchar | 城市键(外键→dim_pub_city_info) |
| taxi_company_key | varchar | 公司键(外键→dim_pub_taxi_company_info) |
| taxi_type_key | varchar | 类型键(外键→dim_pub_taxi_type_info) |
| taxi_longitude | varchar | 经度 |
| taxi_latitude | varchar | 纬度 |
| taxi_speed | double | 速度 |
| taxi_direction | varchar | 方向 |
| taxi_status | varchar | 状态 |

**维度表（6 张）**

| 维度表 | 主键 | 字段 |
|--------|------|------|
| `dim_pub_date_info` | gps_date_key | gps_year, gps_year_ch, gps_month, gps_month_ch, gps_day, gps_day_ch |
| `dim_pub_time_info` | gps_time_key | gps_h, gps_h_ch, gps_m, gps_m_ch, gps_s, gps_s_ch |
| `dim_pub_city_info` | city_key | city_id, city_name, city_status |
| `dim_pub_taxi_base_info` | taxi_key | taxi_number, taxi_color, taxi_brand, taxi_type, taxi_company_key |
| `dim_pub_taxi_type_info` | taxi_type_key | taxi_type_id, taxi_type_name |
| `dim_pub_taxi_company_info` | taxi_company_key | taxi_company_id, taxi_company_name, taxi_company_addr, taxi_company_introduction, taxi_company_complaints, taxi_company_serviceCall |

> 原图部分类型被截断为 `varchar...`/`varchar(0)`；建表时统一主外键类型（建议 `varchar(32)`）以提升 JOIN 性能。
> 存储规划：DIM → Phoenix/HBase；DWS → ClickHouse。

---

# 阶段〇：环境拉起（所有后续步骤的前置）

> 单节点伪分布式，全部以 `wry` 用户启动。

### Task 0.1 启动 ZooKeeper

```bash
cd /opt/zookeeper && bin/zkServer.sh start
# 若提示找不到，确认目录： ls /opt | grep zookeeper
```

**验证**：`bin/zkServer.sh status` → 输出 `Mode: standalone`；`ss -ltn | grep 2181` 有监听。

### Task 0.2 启动 Kafka

```bash
# 先确认 ZK 已起
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
```

**验证**：`jps | grep Kafka` 有进程；`$KAFKA_HOME/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` 能返回（可能是空）。

### Task 0.3 启动 Hadoop（HDFS + YARN）

```bash
# 视安装而定，先确认 HADOOP_HOME
ls /opt/hadoop/bin/start-dfs.sh
/opt/hadoop/bin/start-dfs.sh
/opt/hadoop/bin/start-yarn.sh
```

**验证**：`jps` 看到 `NameNode / DataNode / SecondaryNameNode / ResourceManager / NodeManager`；`hdfs dfs -ls /` 正常。

### Task 0.4 启动 Hive Metastore（后台）

```bash
nohup hive --service metastore > /tmp/hive-metastore.log 2>&1 &
```

**验证**：`jps | grep RunJar`（metastore 进程）；`beeline -u jdbc:hive2://localhost:10000 -e "show databases;"`（若开了 hiveserver2）。最简验证：`hive -e "show databases;"` 不卡住。

### Task 0.5 启动 HBase

```bash
/opt/hbase/bin/start-hbase.sh
```

**验证**：`jps | grep HMaster`；`echo "list" | hbase shell -n`。

### Task 0.6 启动 Flink（standalone）

```bash
/opt/flink-1.17.2/bin/start-cluster.sh
```

**验证**：浏览器/curl `http://localhost:8081` 返回 Flink 仪表盘；`jps | grep StandaloneSessionClusterEntrypoint`。

### Task 0.7 全栈健康检查（一条命令）

```bash
jps | sort
ss -ltn | grep -E '2181|9092|9000|9870|8088|16000|16010|8081|3306|6379'
```

**期望**：ZK/Kafka/NameNode/DataNode/RM/NM/HMaster/HRegionServer/Flask(8081)/MySQL(3306)/Redis(6379) 都在。

---

# 阶段一（L47）准备数据 + Flink CDC

### Task 1.1 准备 MySQL 业务库与 binlog

```bash
# 管理操作走 root（socket 免密）
sudo mysql
```

```sql
-- 建业务库
CREATE DATABASE IF NOT EXISTS shop DEFAULT CHARSET utf8mb4;
USE shop;
CREATE TABLE IF NOT EXISTS orders (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  user_id BIGINT, product_id BIGINT,
  amount DECIMAL(10,2),
  status TINYINT,
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP
);
-- 开启 binlog：确认 my.cnf 有 log-bin / binlog_format=ROW / server-id

-- 创建 Flink CDC 专用账号（需 REPLICATION 权限）
CREATE USER IF NOT EXISTS 'cdc'@'%' IDENTIFIED BY 'CDC_PASSWORD';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'%';
FLUSH PRIVILEGES;
```

**验证**：`SHOW VARIABLES LIKE 'log_bin';` → ON；`SHOW VARIABLES LIKE 'binlog_format';` → ROW。

> 若没开 binlog：编辑 `/etc/mysql/mysql.conf.d/mysqld.cnf` 加
> `server-id=1`、`log_bin=/var/log/mysql/mysql-bin.log`、`binlog_format=ROW`、`binlog_row_image=FULL`，然后 `sudo systemctl restart mysql`。

### Task 1.2 安装 Flink CDC 连接器（2.4.2）

```bash
cd /opt/flink-1.17.2/lib
# 国内可加镜像前缀，例如 https://mirrors.aliyun.com/... 视可达性
wget https://repo1.maven.org/maven2/com/ververica/flink-sql-connector-mysql-cdc/2.4.2/flink-sql-connector-mysql-cdc-2.4.2.jar
```

**验证**：`ls -l lib/ | grep cdc`。

### Task 1.3 Flink SQL 跑通 CDC 源（打印到 console）

```sql
-- start-cluster 后，用 sql-client
/opt/flink-1.17.2/bin/sql-client.sh embedded
```

```sql
CREATE TABLE mysql_orders (
  id BIGINT, user_id BIGINT, product_id BIGINT, amount DECIMAL(10,2),
  status TINYINT, create_time TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector'='mysql-cdc','hostname'='localhost','port'='3306',
  'username'='cdc','password'='CDC_PASSWORD',
  'database-name'='shop','table-name'='orders',
  'server-time-zone'='UTC'      -- 必须与 MySQL 服务器时区一致（本机为 UTC）
);
SELECT * FROM mysql_orders;
```

**验证**：在 MySQL `INSERT` 一行，Flink 控制台实时打印该行。

---

# 阶段二（L48）Flink CDC + Hudi 编译

### Task 2.1 安装 Maven（编译 Hudi 需要）

```bash
sudo apt-get install -y maven    # 或下载 apache-maven-3.9.x-bin.tar.gz 解压
mvn -v
```

### Task 2.2 获取 Hudi Flink Bundle

**路线 A（快，推荐先跑通）**：直接下预编译包（Hudi 0.14.1，Flink 1.17 / Scala 2.12）

```bash
cd /opt/flink-1.17.2/lib
wget https://repo1.maven.org/maven2/org/apache/hudi/hudi-flink1.17-bundle/0.14.1/hudi-flink1.17-bundle-0.14.1.jar
```

**路线 B（课程"编译"，忠于视频）**：

```bash
git clone https://github.com/apache/hudi.git -b release-0.14.1
cd hudi
mvn clean package -DskipTests -Djava.version=8 \
  -pl hudi-flink-datasource/hudi-flink -am
# 产物：hudi-flink-datasource/hudi-flink/target/hudi-flink1.17-bundle-0.14.1.jar
cp hudi-flink-datasource/hudi-flink/target/hudi-flink1.17-bundle-0.14.1.jar /opt/flink-1.17.2/lib/
```

**验证**：`ls -l /opt/flink-1.17.2/lib | grep hudi`。

### Task 2.3 重启 Flink、验证 Hudi 连接器加载

```bash
/opt/flink-1.17.2/bin/stop-cluster.sh && /opt/flink-1.17.2/bin/start-cluster.sh
```

**验证**：sql-client 里 `CREATE TABLE ... WITH ('connector'='hudi', ...)` 不再报 "connector not found"。

---

# 阶段三（L49~L50）Hudi + FlinkSQL + ODS

### Task 3.1 创建 ODS 湖表（Hudi，写入 HDFS）

```sql
CREATE TABLE ods_orders (
  id BIGINT, user_id BIGINT, product_id BIGINT, amount DECIMAL(10,2),
  status TINYINT, create_time TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector'='hudi',
  'path'='hdfs://localhost:9000/warehouse/ods/orders',
  'table.type'='MERGE_ON_READ',
  'write.operation'='upsert',
  'hoodie.datasource.write.recordkey.field'='id',
  'hoodie.datasource.write.precombine.field'='create_time'
);
```

### Task 3.2 MySQL → ODS 落湖

```sql
INSERT INTO ods_orders SELECT * FROM mysql_orders;
```

**验证**：`hdfs dfs -ls /warehouse/ods/orders` 有 parquet/log 文件；MySQL 里 update 一行，Hudi 表随之更新。

### Task 3.3 ODS 增量查询

```sql
SELECT * FROM ods_orders /*+ OPTIONS('read.streaming.enabled'='true') */;
```

---

# 阶段四（L51~L54）数仓模型 / 指标体系 / 维度模型 / 分层规划

> 这几节以**设计**为主，重点是产出分层表结构。建议先落成 SQL 文件，方便后续复用。

### Task 4.1 数仓分层与表清单（设计）

```
事实表: taxi_gps_detail (出租车GPS明细, 11字段)
维度表: dim_pub_date_info / dim_pub_time_info / dim_pub_city_info
        / dim_pub_taxi_base_info / dim_pub_taxi_type_info / dim_pub_taxi_company_info
ODS: ods_vehicle_od / ods_vehicle_realtime / ods_chase_vehicle / ods_violation_vehicle (Hudi)
DIM: 6 张 dim_pub_* (Phoenix/HBase)
DWM: 事实+维度 拉宽 (Phoenix/Redis)
DWS: 车速/流量/OD 指标聚合 (ClickHouse)
ADS: 实时大屏指标 (Flume→/Redis/WS)
```

### Task 4.2 指标体系（设计）

- **指标类型**：原子指标（最底不可再分）· 派生指标（原子+修饰词+时间周期）· 事物型（记录状态变更）· 存量型（记录状态）· 复合型（派生再延伸）
- **指标体系四性**（有效性判断）：完备性（不遗漏）· 系统性（反映业务问题/定位目标）· 可执行性（系统可操作）· 可解释性（定义清晰通俗）
- **交通域常见指标**：车辆数、GPS 点数、平均车速、OD 出行量、违章数、追逐识别数
- **常见坑**：同名不同口径 / 同口径不同名 / 口径描述不清 / 名称写错或重复
- **搭建原则**：有重点、解决实际问题、以理解业务为基础（不是越全越好）
- 维度：时间(日/时)、地域(城市/交通小区)、车辆(公司/车型/颜色)

### Task 4.3 维度模型（设计）

- 课程用 **星型模型**：中心事实 `taxi_gps_detail` + 6 张 `dim_pub_*` 环绕
- 四类核心维度：**产品 / 用户 / 时间 / 地域**（本项目映射为 车辆 / 公司 / 时间 / 城市&交通小区）
- **星型 vs 雪花**：星型直观、JOIN 少、查询快；雪花把维度再规范化、减冗余但表关系多、查询复杂
- 流程：数据调研 → 数据域划分 → 统计口径/指标规范定义 → 模型落地
- 实时数仓可扮演 **Lambda/Kappa** 角色：增量(Hudi 流式) + 全量(Hudi 批)统一

### Task 4.3b 维度建模进阶（L53）

- **维度拆分/整合**：拆分→更细粒度（应对属性变化）；整合→多表合一（简化查询）
- **缓慢变化维 SCD**：① 重写（不留历史，用最新）② 插新行（保留历史）③ 拉链表（新旧关联）
  - 实践：**快照**（每天全量一份 + 限定词关联）vs **拉链表**（加生效/失效时间戳），可用视图透明化
- **微型维度 / 卫星维度**：把常变或小规模维度单拉出来（如会员等级、商品等级）
- **递归层次**：扁平化（展平成字段）or 桥接/调节表（处理非均衡层次，如菜单/类别树）
- **多值属性**：KV 存一列 / 拆多列 / 一条记录代表一个属性值
- **事实表设计**：粒度（业务细节程度）；可加/半可加/不可加度量；退化维度（维度属性放进事实表）；事实表三型 = 事务型 / 周期快照型 / 累积快照型
- **元数据**：数据字典（结构）、数据血缘（表从哪来，影响分析/溯源）、数据特征（大小/热度/主题域）；工具 Apache Atlas 等；采集方式 = 静态解析 / 执行计划解析 / 日志解析
- **数据埋点**：客户端 vs 服务端；可视化 vs 代码全埋点
- **数据同步**：直连同步 / 文件同步 / 数据库日志解析（binlog）；注意**数据漂移**（延时/缺失），用分区存储解决

### Task 4.4 分层开发落地

把 4.1 里每张表的 `CREATE TABLE` 落成文件：`/home/wry/warehouse/sql/*.sql`。

### Task 4.5 星型模型建表（MySQL `traffic` 库）

按课程模型图建：事实表 `taxi_gps_detail` + 6 张 `dim_pub_*`（DDL 见「数仓模型」小节）。
- 主外键类型统一（建议 `varchar(32)`）以利 JOIN
- 维表（日期/时间/城市/公司/车型/出租车基础）灌少量样例数据

### Task 4.6 Hudi → Hive 同步（课程重点）

给 Hudi ODS 表加 hive_sync 参数，让 Hive/Impala 能查：

```sql
WITH (
  'connector'='hudi','path'='hdfs://bigdata-live01-01:9000/warehouse/ods/vehicle_od',
  'table.type'='MERGE_ON_READ',
  'write.precombine.field'='OrgTimeStamp',
  -- 写时同步到 Hive metastore
  'hive_sync.enable'='true',
  'hive_sync.mode'='hms',                                  -- 直连 metastore
  'hive_sync.metastore.uris'='thrift://bigdata-live01-01:9083',
  'hive_sync.db'='traffic',
  'hive_sync.table'='ods_vehicle_od',
  'hive_sync.support_timestamp'='true',
  -- MOR 表：异步 compaction
  'compaction.async.enabled'='true',
  'compaction.schedule.enabled'='true'
)
```

**验证**：`hive -e "show partitions traffic.ods_vehicle_od; select count(*) from traffic.ods_vehicle_od;"`。
> 坑：hive_sync 需 Hive 的 `hive-site.xml` 或用 `hive_sync.metastore.uris` 直连；metastore 要运行（tmux 会话 `hive`）。

### Task 4.7 维表初始化（HBase/Redis）

- 轻量维表 → Redis；大量维度 → HBase/Phoenix
- 参考已有 `RedisOffsetConsumer` 的 JedisPool 用法
- 维度建模：**星型**（事实+维表，直观）vs **雪花**（维度再规范化，减冗余但查询复杂）——课程用星型

### Task 4.8 Table/Stream 互转 + DWD(UDF/过滤/聚合)

- Flink 里 `toChangelogStream` / `toDataStream` 互通
- DWD：UDF 转换、清洗过滤、轻度聚合
- DWS：细粒度/主题汇总

### Task 4.9 工程规范与代码结构（L54，代码阶段）

**数仓分层**：ODS / DWD / DWS / ADS / DIM / **Temp**（临时层）

**命名规范**：`{层}_{业务系统}_{装载策略}_{装载周期}_{业务表}`
- 例：`ods_mysql_binlog_taxi_gps_info`
- 增量数据加 `i`，实时数据加 `rt`

**维表设计**：日期维（工作日/周末/假日/季节）· 文本属性维（单双号/限行）· 产品维（名称/信息/标准价格）

**工程结构（Java + SQL）**：
- `SqlFilePath` 常量类：集中管理 SQL 文件路径
- `readSqlFile` 工具类：读 SQL 文件、按 `;` 切分成语句列表（正则匹配分号结尾、去注释）
- `readProperties` 工具类：加载 Property 配置（DB 连接、业务参数），处理异常
- SQL 文件 + `.properties` 配置文件分离
- ODS 层：建表 + 加载 + `transform` 转换；用**视图**简化/复用查询

**开发策略**：先写完整逻辑，再统一测试调试（先写后测）

---

# 阶段五（L55）ODS + DIM

### Task 5.1 ODS→DIM 抽取（用户/商品维度）

```sql
-- 从 ODS 或 MySQL 全量+增量同步到 DIM（HBase/Phoenix 表）
```

**验证**：`dim_user` / `dim_product` 有数据。

---

# 阶段六（L56）Phoenix + DIM

### Task 6.1 安装 Phoenix（匹配 HBase 2.3.2）

```bash
cd /opt
wget https://archive.apache.org/dist/phoenix/apache-phoenix-5.1.2-HBase-2.3-bin.tar.gz
tar xzf apache-phoenix-5.1.2-HBase-2.3-bin.tar.gz
mv apache-phoenix-5.1.2-HBase-2.3-bin phoenix
# 拷贝 server jar 到 HBase lib
cp /opt/phoenix/phoenix-server-hbase-2.3-5.1.2.jar /opt/hbase/lib/
/opt/hbase/bin/stop-hbase.sh && /opt/hbase/bin/start-hbase.sh
```

### Task 6.2 建 DIM 表并用 sqlline 验证

```bash
/opt/phoenix/bin/sqlline.py localhost:2181
```
```sql
CREATE TABLE IF NOT EXISTS dim_user (user_id BIGINT PRIMARY KEY, name VARCHAR, age INTEGER, city VARCHAR);
UPSERT INTO dim_user VALUES (1,'张三',28,'bj');
SELECT * FROM dim_user;
```

**验证**：能查到刚 upsert 的行。

---

# 阶段七（L57）Phoenix + DWM

### Task 7.1 建 DWM 宽表（Phoenix）

```sql
CREATE TABLE IF NOT EXISTS dwm_order_detail (
  order_id BIGINT PRIMARY KEY, user_name VARCHAR, product_name VARCHAR,
  amount DECIMAL(10,2), ts TIMESTAMP
);
```

### Task 7.2 Flink SQL JOIN DIM 生成 DWM

```sql
-- ods_orders JOIN 维表 dim_user/dim_product → 写 dwm_order_detail
```

**验证**：DWM 表随 ODS 新数据增长。

---

# 阶段八（L58）Phoenix + Redis + DWM

### Task 8.1 Redis 存 DWM 热点/实时态

```bash
redis-cli -h 127.0.0.1 -p 6379 ping     # 期望 PONG
```

### Task 8.2 把 DWM 结果写 Redis（Flink redis connector / 自定义 sink）

```java
// 参考已有 RedisOffsetConsumer 里的 JedisPool 用法
// key: dwm:order:{id}  value: 宽表 JSON
```

**验证**：`redis-cli hgetall dwm:order:1` 有值。

---

# 阶段九（L59）ClickHouse + DWS

### Task 9.1 安装 ClickHouse

```bash
sudo apt-get install -y clickhouse-server clickhouse-client   # 或官方 deb 离线包
sudo systemctl start clickhouse-server
clickhouse-client --query "SELECT 1"
```

### Task 9.2 建 DWS 表并做窗口聚合

```sql
CREATE TABLE dws_order_daily (
  dt Date, total_amount Float64, order_count UInt64, user_count UInt64
) ENGINE = SummingMergeTree() ORDER BY dt;
```

```sql
-- Flink SQL 窗口聚合(按天/小时) → 写 ClickHouse
```

**验证**：`SELECT * FROM dws_order_daily` 有聚合结果。

---

# 阶段十（L60）ADS + DWS

### Task 10.1 ADS 汇总表

```sql
-- 实时大屏指标：今日 GMV、订单数、在线人数
```

**验证**：ADS 指标随数据更新。

---

# 阶段十一（L61）ADS + Flume

### Task 11.1 安装并配置 Flume

```bash
cd /opt && wget https://archive.apache.org/dist/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz
tar xzf apache-flume-1.11.0-bin.tar.gz && mv apache-flume-1.11.0-bin flume
```

### Task 11.2 Flume 采集日志 → Kafka/HDFS

`conf/ads-to-kafka.conf`：`source=taildir → channel=memory → sink=kafka`，并 `./bin/flume-ng agent -n a1 -f conf/ads-to-kafka.conf -Dflume.root.logger=INFO,console`。

**验证**：往被 tail 的文件里追加一行，Kafka 对应 topic 能消费到。

---

# 阶段十二（L62）WS + WebSocket

### Task 12.1 起一个 WebSocket 服务推 ADS 实时指标

```java
// 用 Flink 结果 → WebSocket(如 Java-WebSocket / Spring) → 前端实时大屏
```

**验证**：浏览器连上 WS，能持续收到 GMV/订单数推送。

---

# 风险与依赖

1. **外网慢**：Hudi/Phoenix/Flume/ClickHouse 下载量大，国内可能很慢。优先用镜像；必要时用宿主机代理（宿主机有 Clash :7897）。
2. **无 Maven**：L48 若走"源码编译"必须先装 Maven，且编译耗时长、依赖多。
3. **内存**：单虚机同时跑 ZK+Kafka+HDFS+YARN+HBase+Phoenix+Flink+ClickHouse 很吃内存，注意 `-Xmx` 与 JVM 堆，必要时错峰启动。
4. **版本匹配**：Phoenix 必须 `5.1.2-HBase-2.3`（对上 HBase 2.3.2）；Hudi 用 `0.14.1`、CDC 用 `2.4.2`（对上 Flink 1.17 / Scala 2.12）。
5. **非 root 运行 HDFS/YARN**：以 `wry` 用户起，注意目录属主。

---

# 执行顺序（建议）

```
阶段〇(环境拉起) → 阶段一(L47) → 阶段二(L48) → 阶段三(L49-50)
→ 阶段四(L51-54,设计) → 阶段五(L55) → 阶段六(L56) → 阶段七(L57)
→ 阶段八(L58) → 阶段九(L59) → 阶段十(L60) → 阶段十一(L61) → 阶段十二(L62)
```
