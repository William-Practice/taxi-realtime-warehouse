-- L47：Flink CDC 流式验证（changelog 模式）
-- 存量数据会以 +I 打印，之后持续监听 MySQL 的增删改
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE mysql_orders (
  id BIGINT,
  user_id BIGINT,
  product_id BIGINT,
  amount DECIMAL(10,2),
  status TINYINT,
  create_time TIMESTAMP(3),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname'  = 'localhost',
  'port'      = '3306',
  'username'  = 'cdc',
  'password'  = 'CDC_PASSWORD',
  'database-name' = 'shop',
  'table-name'    = 'orders'
);

SELECT * FROM mysql_orders;
