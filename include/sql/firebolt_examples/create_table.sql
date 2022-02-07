CREATE FACT TABLE IF NOT EXISTS {{ params.table }} (
  id INT,
  order_name String,
  order_num INT,
  quantity INT,
  price FLOAT,
  order_date String,
  order_datetime DATETIME
)
PRIMARY INDEX id;
