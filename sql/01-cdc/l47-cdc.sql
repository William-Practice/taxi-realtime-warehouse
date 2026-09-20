-- L47：Flink CDC 验证（batch 模式，快照读一次即退出，证明连得通 MySQL）
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'table';

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
