-- L47：Flink CDC → print sink（结果写到 TaskManager stdout，便于验证）
SET 'execution.runtime-mode' = 'streaming';

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
  'password'  = 'Cdc@123456',
  'database-name' = 'shop',
  'table-name'    = 'orders',
  'scan.startup.mode' = 'initial',
  'server-time-zone'  = 'UTC'
);

CREATE TABLE print_orders (
  id BIGINT,
  user_id BIGINT,
  product_id BIGINT,
  amount DECIMAL(10,2),
  status TINYINT,
  create_time TIMESTAMP(3)
) WITH (
  'connector' = 'print',
  'print-identifier' = 'L47CDC'
);

INSERT INTO print_orders SELECT * FROM mysql_orders;
