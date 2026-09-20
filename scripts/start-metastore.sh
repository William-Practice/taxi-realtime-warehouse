#!/bin/bash
# 启动 Hive Metastore（供 Flink Hive Catalog / Hive 使用）
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export HADOOP_PREFIX=/opt/hadoop
export HIVE_HOME=/opt/apache-hive-3.1.0-bin
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HIVE_HOME/bin
exec $HIVE_HOME/bin/hive --service metastore
