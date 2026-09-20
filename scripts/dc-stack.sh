#!/bin/bash
# dc-stack.sh — 实时数仓基础栈一键启停（在 bigdata-live01-01 上执行）
# 用法: ./dc-stack.sh {start|stop|restart|status}
set -u
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_PREFIX=/opt/hadoop
export HIVE_HOME=/opt/apache-hive-3.1.0-bin
export KAFKA_HOME=/opt/kafka
export ZOOKEEPER_HOME=/opt/zookeeper
export HBASE_HOME=/opt/hbase
export FLINK_HOME=/opt/flink-1.17.2
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin:$KAFKA_HOME/bin:$ZOOKEEPER_HOME/bin:$HBASE_HOME/bin:$FLINK_HOME/bin

port_check() { ss -ltn | grep -q ":$1 " && echo "  :$1  UP" || echo "  :$1  down"; }

start() {
  echo ">> ZooKeeper";        $ZOOKEEPER_HOME/bin/zkServer.sh start >/dev/null 2>&1; sleep 3
  echo ">> Kafka";            $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties; sleep 6
  echo ">> HDFS + YARN";      $HADOOP_HOME/sbin/start-dfs.sh; $HADOOP_HOME/sbin/start-yarn.sh; sleep 4
  echo ">> Hive Metastore (tmux 会话 hive)"
  tmux has-session -t hive 2>/dev/null || tmux new-session -d -s hive "$HIVE_HOME/bin/hive --service metastore"; sleep 2
  echo ">> HBase";            $HBASE_HOME/bin/start-hbase.sh; sleep 6
  echo ">> Flink";            $FLINK_HOME/bin/start-cluster.sh; sleep 3
  echo ">> 完成。执行 $0 status 查看。"
}

stop() {
  echo ">> Flink";            $FLINK_HOME/bin/stop-cluster.sh >/dev/null 2>&1
  echo ">> HBase";            $HBASE_HOME/bin/stop-hbase.sh >/dev/null 2>&1; sleep 3
  echo ">> Hive Metastore";   tmux kill-session -t hive 2>/dev/null
  echo ">> YARN + HDFS";      $HADOOP_HOME/sbin/stop-yarn.sh >/dev/null 2>&1; $HADOOP_HOME/sbin/stop-dfs.sh >/dev/null 2>&1
  echo ">> Kafka";            $KAFKA_HOME/bin/kafka-server-stop.sh >/dev/null 2>&1; sleep 3
  echo ">> ZooKeeper";        $ZOOKEEPER_HOME/bin/zkServer.sh stop >/dev/null 2>&1
  echo ">> 完成。"
}

status() {
  echo "== jps =="; jps | sort -k2
  echo "== 关键端口 =="
  for p in 2181 9092 9000 9870 8088 9083 16000 16010 8083; do port_check $p; done
  echo "== tmux 会话 =="; tmux ls 2>/dev/null || echo "  (无)"
}

case "${1:-}" in
  start)   start ;;
  stop)    stop ;;
  restart) stop; sleep 3; start ;;
  status)  status ;;
  *) echo "用法: $0 {start|stop|restart|status}"; exit 1 ;;
esac
