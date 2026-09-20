# 出租车 GPS 实时数仓（Paimon 湖仓版）

[![CI](https://github.com/William-Practice/taxi-realtime-warehouse/actions/workflows/ci.yml/badge.svg)](https://github.com/William-Practice/taxi-realtime-warehouse/actions/workflows/ci.yml)
![Flink](https://img.shields.io/badge/Flink-1.17.2-E6526F?logo=apacheflink&logoColor=white)
![Paimon](https://img.shields.io/badge/Paimon-0.8.0-1E88E5)
![Kafka](https://img.shields.io/badge/Kafka-3.2.0-231F20?logo=apachekafka&logoColor=white)
![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green)

> 一个**端到端跑通**的实时数仓项目：MySQL 业务库 → Flink CDC → Paimon 湖仓（ODS/DIM/DWM/DWS）→ ADS → WebSocket 实时大屏。
> 本 README 可直接作为简历项目 / 面试讲解材料。
> 姊妹仓库（Flink / Kafka 实验代码与踩坑记录）：[flink-kafka-labs](https://github.com/William-Practice/flink-kafka-labs)

---

## 一、项目简介

面向**交通/出租车 GPS**场景（OD 出行分析），构建一套**流批一体的实时数仓**：

- 实时捕获 MySQL 业务变更（Flink CDC 读 binlog）
- 湖仓分层存储（Apache Paimon：ODS / DIM / DWM / DWS）
- 维度建模（星型模型：1 事实表 + 6 维表）
- 流式聚合 + 维度 lookup join
- ADS 结果实时推送到前端大屏（WebSocket）

**一句话**：`MySQL(binlog) → Flink CDC → Paimon(湖仓) → 实时聚合 → ADS(MySQL) → WS 大屏`

---

## 仓库结构

```
taxi-realtime-warehouse/
├── README.md
├── docs/
│   └── implementation-plan.md      # L47~L62 实施计划 + 逐阶段进展 + 踩坑记录
├── sql/
│   ├── 00-setup/                   # 业务表 + cdc 账号
│   ├── 01-cdc/                     # Flink CDC 验证
│   ├── 02-ods-paimon/              # 阶段A: CDC → Paimon ODS
│   ├── 03-star-model/              # 阶段B: 星型模型 + DIM 加载
│   ├── 04-dwm-dws/                 # 阶段C: 宽表 + 聚合
│   ├── 05-ads/                     # 阶段D: ADS + 真实时流式作业
│   ├── 06-hive-sync/               # Paimon → Hive 同步
│   └── legacy-hudi/                # 旧 Hudi 版（供对照）
├── scripts/
│   ├── dc-stack.sh                 # 基础栈一键起停
│   ├── start-metastore.sh          # Hive Metastore 启动
│   ├── ads_ws.py                   # ADS WebSocket 推送服务
│   └── ws_test.py                  # WS 测试客户端
└── web/
    └── ads_dashboard.html          # 实时大屏前端
```

---

## 二、技术栈

| 组件 | 版本 | 作用 |
|------|------|------|
| Flink | 1.17.2 | 流式计算引擎（SQL 为主） |
| Flink CDC | 2.4.2 | MySQL binlog 实时捕获 |
| **Apache Paimon** | **0.8.0** | 流批一体湖仓（ODS/DIM/DWM/DWS） |
| MySQL | 8.x | 业务库（源）+ ADS serving |
| Redis | — | 热维（预留） |
| Python | 3.10 | WebSocket 推送服务 + 大屏 |
| HDFS | 3.2.0 | Paimon 底层存储 |

> 注：本方案是对一套**老课程栈**的现代化改造 —— 湖由 Hudi 换为 **Paimon**，OLAP 由 ClickHouse/Phoenix 换为 **Paimon 内嵌查询 + MySQL serving**（原 VM 内存仅 3.8G，装不下 Doris）。

---

## 三、整体架构

```
                 MySQL (binlog, ROW)
                       │
              Flink CDC │ (增量 + 快照)
                       ▼
   ┌──────────────────────────────────────┐
   │           Apache Paimon (湖仓)          │
   │  ODS: ods_taxi_gps_detail / ods_vehicle_od │
   │  DIM: dim_pub_* ×6                       │
   │  DWM: dwm_taxi_gps_wide (宽表)            │
   │  DWS: dws_city_traffic (聚合)             │
   └──────────────────────────────────────┘
                       │  流式聚合 + 维表 lookup join
                       ▼
        MySQL.ads_city_realtime (ADS, 持续 UPSERT)
                       │  Python WebSocket (2s 轮询推送)
                       ▼
                浏览器实时大屏 (总GPS/车辆/车速/载客)
```

**数据分层**：ODS（原始落湖）→ DIM（维度）→ DWM（宽表拉宽）→ DWS（指标聚合）→ ADS（应用/大屏）。

---

## 四、数据模型（星型）

**事实表 `taxi_gps_detail`**（出租车 GPS 明细）：

| 字段 | 说明 |
|------|------|
| taxi_key | 出租车代理键 |
| gps_date_key / gps_time_key | 日期键 / 时间键 |
| city_key / taxi_company_key / taxi_type_key | 城市 / 公司 / 车型键 |
| taxi_longitude / taxi_latitude | 经度 / 纬度 |
| taxi_speed | 车速 |
| taxi_direction / taxi_status | 方向 / 状态 |

**维度表 ×6**：`dim_pub_date_info`（日期）、`dim_pub_time_info`（时间）、`dim_pub_city_info`（城市）、`dim_pub_taxi_base_info`（车辆基础）、`dim_pub_taxi_type_info`（车型）、`dim_pub_taxi_company_info`（公司）。

**源表（ODS 来源）**：`vehicle_od`（OD 数据）、`vehicle_realtime`（实时位置）、`chase_vehicle`（追逐识别）、`violation_vehicle`（违章）。

---

## 五、关键实现要点

1. **Flink CDC 捕获 binlog**：`mysql-cdc` 连接器；表无主键时用 `scan.incremental.snapshot.enabled=false`（legacy 快照）；`server-time-zone` 必须与 MySQL 一致（本机 UTC）。
2. **Paimon 湖仓**：`CREATE CATALOG paimon`（主键表 + `bucket`），ODS 用流式写入、DIM 用 batch 初始化；Paimon 提交靠 checkpoint。
3. **维度 lookup join**：事实流带 `PROCTIME()`，维表用 `FOR SYSTEM_TIME AS OF o.proc_time` 做实时维表关联（Paimon DIM lookup）。
4. **流式聚合**：`GROUP BY 城市+日期`，指标 = GPS点数 / 车辆数 / 平均车速 / 载客次数。
5. **ADS 持续写**：Flink JDBC sink 以 **UPSERT** 方式持续写 MySQL。
6. **WebSocket 大屏**：Python `websockets` 每 2s 轮询 ADS 并推送；前端 HTML 实时渲染 + 汇总卡片。

---

## 六、环境与文件

**虚机**：`bigdata-live01-01`（192.168.56.101，用户 wry）

**关键组件路径**
- Flink：`/opt/flink-1.17.2`（UI: `http://192.168.56.101:8083`）
- Flink lib 额外 jar：`paimon-flink-1.17-0.8.0.jar`、`flink-sql-connector-mysql-cdc-2.4.2.jar`、`flink-connector-jdbc-3.1.2-1.17.jar`、`mysql-connector-j-8.0.33.jar`、`flink-shaded-hadoop-2-uber-2.8.3-10.0.jar`
- HDFS：`/opt/hadoop`，Paimon warehouse：`hdfs://bigdata-live01-01:9000/warehouse/paimon`

**项目文件**
| 文件 | 说明 |
|------|------|
| `/home/wry/dc-stack.sh` | 基础栈一键起停（ZK/Kafka/HDFS/HBase/HiveMetastore/Flink） |
| `/home/wry/*.sql` | 各阶段 Flink SQL（建表 / CDC / 聚合） |
| `/home/wry/ads_ws.py` | ADS WebSocket 推送服务（:8765） |
| `/home/wry/web/ads_dashboard.html` | 实时大屏页 |

---

## 七、启动步骤（TL;DR）

```bash
# 1) 基础栈
/home/wry/dc-stack.sh start        # ZK/Kafka/HDFS/Metastore/Flink

# 2) MySQL 侧（业务表 + 维表 + 样例数据 + cdc 账号）
sudo mysql < /home/wry/b-star.sql  # 星型模型
sudo mysql < /home/wry/d-ads-ddl.sql

# 3) 湖仓：ODS(流) + DIM(batch)
sql-client.sh -f pa-ods.sql        # CDC → Paimon ODS
sql-client.sh -f b-dim.sql         # 维表 → Paimon DIM

# 4) 实时：CDC → lookup join → 聚合 → ADS(MySQL)
sql-client.sh -f d2-stream.sql     # 前台常驻（流式作业）

# 5) 大屏
python3 /home/wry/ads_ws.py &                                   # :8765
cd /home/wry/web && python3 -m http.server 8080 &               # :8080
# 浏览器打开 http://192.168.56.101:8080/ads_dashboard.html
```

---

## 八、成果 & 亮点

- ✅ 全链路跑通：CDC → 湖仓 → 数仓分层 → 实时聚合 → 大屏，**数据可读回验证**
- ✅ **实时**：插入 1 条 GPS 明细，ADS/大屏 ~10s 内自动更新
- ✅ 星型模型 + 6 维 lookup join
- ✅ 用 **Paimon** 替代 Hudi、Paimon+MySQL 替代 ClickHouse/Phoenix —— 更贴当前流批一体湖仓趋势

---

## 九、踩坑记录（工程经验）

1. **HDFS 副本**：`dfs.replication=2` 且从节点 DataNode 故障 → `Failed to replace a bad datanode ... pipeline` → 湖表 append 失败。改 `dfs.replication=1`。
2. **Flink slot**：默认 `numberOfTaskSlots=1` 不够，多算子报 `NoResourceAvailableException`，改 4。
3. **Paimon 建表**：必须走 `CREATE CATALOG`（`'type'='paimon'`），不能只用 `'path'`（否则 `Schema file not found`）。
4. **Paimon Sink 并行度**：不支持 Flink batch 自适应并行度 → `SET 'execution.batch.adaptive.auto-parallelism.enabled'='false'`。
5. **`default` 保留字**：跨 catalog 引用要反引号 `` `paimon`.`default`.`tbl` ``。
6. **CDC 时区**：`server-time-zone` 必须 = MySQL 服务器时区（UTC）。
7. **checkpoint 间隔**：湖表提交慢，间隔太小（10s）会 checkpoint declined / 作业重启，放大到 30s。

---

## 十、Paimon → Hive 同步（可选）

让 Hive 能直接查询 Paimon 湖表（离线分析 / 血缘常用）。两种方式：

**方式一：`hive_sync` 表属性** —— 见 `sql/06-hive-sync/01-hive-sync-options.sql`。

**方式二：Hive-backed Paimon Catalog（推荐，表直接注册进 Hive metastore）**

```sql
CREATE CATALOG paimon_hive WITH (
  'type'      = 'paimon',
  'metastore' = 'hive',
  'uri'       = 'thrift://bigdata-live01-01:9083',
  'warehouse' = 'hdfs://bigdata-live01-01:9000/warehouse/paimon_hive'
);
```

依赖：
- Hive 侧：把 `paimon-hive-connector-3.1-0.8.0.jar` + `paimon-hive-catalog-0.8.0.jar` 放入 `$HIVE_HOME/auxlib/`
- Flink 侧 lib：`flink-sql-connector-hive-2.3.9_2.12-1.17.2.jar`（提供 HiveConf 等）

验证（Hive 直接读 Paimon 表）：
```sql
hive> SELECT * FROM dws_city_traffic;
上海  20260920  3  2  25.17  2
北京  20260920  1  1  62.3   1
```

---

## 十一、简历写法（模板）

```
项目：出租车 GPS 实时数仓（流批一体）
技术栈：Flink 1.17 + Flink CDC + Apache Paimon + MySQL + WebSocket
职责/亮点：
• 基于 Flink CDC 实时捕获 MySQL binlog，构建 ODS→DIM→DWM→DWS→ADS 分层数仓
• 采用 Apache Paimon 作为流批一体湖仓，落地 ODS 明细与 DIM 维度
• 事实流与维表通过 PROCTIME lookup join 实时关联，流式聚合生成城市级交通指标
• 聚合结果持续 UPSERT 到 MySQL，经 WebSocket 推送前端实时大屏，端到端延迟 ~10s
• 独立完成环境搭建、组件版本选型、链路排障（HDFS 副本 / Flink 并行度 / 湖表提交等）
```

---

## 十二、CI 与代码质量

每次 push / PR 自动跑 4 个 job（配置见 [`.github/workflows/ci.yml`](.github/workflows/ci.yml)）：

| Job | 做什么 |
|-----|--------|
| **仓库结构检查** | 关键文件（README / LICENSE / 大屏 / 实施计划）是否齐全；`sql/00-setup`~`sql/06-hive-sync` 七层目录是否存在、每层是否有 SQL、编号层级是否完整 |
| **Shell 脚本检查** | `shellcheck -S error` 检查 `scripts/*.sh` |
| **Python 脚本检查** | `py_compile` + `pyflakes` 检查 `scripts/*.py` |
| **敏感信息扫描** | 扫 GitHub PAT / OpenAI / AWS / Slack / Google / 私钥等**真实凭据特征**，并阻止提交 `*.env` `*.pem` `*.key` 等敏感文件 |

> 关于密码：仓库里的 MySQL 实验账号密码是**故意保留**的 —— 它们只用于 Host-Only 网络（`192.168.56.0/24`）中的
> VirtualBox 虚拟机，外网不可达，且已在 `docs/implementation-plan.md` 里作为环境搭建说明公开。
> 因此 CI 的密钥扫描只针对"真实可用的凭据"，不做无差别的 `password` 字段匹配。

---

## 十三、凭据说明

仓库**不保存任何真实凭据**：

- `sql/**/*.sql` 与 `docs/*.md` 里的 `CDC_PASSWORD` / `HIVE_PASSWORD` / `SQOOP_PASSWORD`
  都是**占位符**。本地实验要用时一行命令替换：

  ```bash
  find sql -name '*.sql' -exec sed -i "s/CDC_PASSWORD/<你的密码>/g" {} +
  ```

- `scripts/ads_ws.py` 已改为从环境变量读取，不需要替换：

  ```bash
  MYSQL_PASSWORD='你的密码' python3 scripts/ads_ws.py
  # 支持 MYSQL_HOST / MYSQL_PORT / MYSQL_USER / MYSQL_PASSWORD / MYSQL_DATABASE
  ```

- CI 的「敏感信息扫描」job 会持续拦截真实凭据（GitHub PAT / OpenAI / AWS / Slack / Google / 私钥）
  以及 `*.env`、`*.pem`、`*.key` 之类的文件被误提交。

---

> 附：详细实施计划与逐阶段进展见同目录 `L47-L62-实时数仓项目实施计划.md`。
