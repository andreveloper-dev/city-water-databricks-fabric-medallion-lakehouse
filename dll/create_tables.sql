-- bronze_orders
CREATE TABLE IF NOT EXISTS bronze_orders (
  latitude DOUBLE,
  longitude DOUBLE,
  date STRING,
  customer_id STRING,
  employee_id STRING,
  quantity_products INT,
  order_id STRING
) USING DELTA;

-- silver_orders
CREATE TABLE IF NOT EXISTS silver_orders (
  partition_date STRING,
  order_id STRING,
  customer_id STRING,
  employee_id STRING,
  quantity_products INT,
  latitude DOUBLE,
  longitude DOUBLE,
  district STRING,
  neighborhood STRING,
  event_date STRING,
  event_year INT,
  event_month INT,
  event_day INT,
  event_hour INT,
  event_minute INT,
  event_second INT
) USING DELTA;

-- gold_performace_operation
CREATE TABLE IF NOT EXISTS gold_performace_operation (
  approved_date DATE,
  total_orders BIGINT,
  total_products BIGINT,
  avg_products_per_order DOUBLE
) USING DELTA;

-- gold_performance_employees
CREATE TABLE IF NOT EXISTS gold_performance_employees (
  employee_id STRING,
  total_products BIGINT,
  performance_quartile INT,
  name STRING,
  comission DOUBLE,
  commission_amount DOUBLE
) USING DELTA;

-- gold_location_employees
CREATE TABLE IF NOT EXISTS gold_location_employees (
  employee_id STRING,
  district STRING,
  neighborhood STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  last_location TIMESTAMP,
  name STRING,
  phone STRING,
  email STRING
) USING DELTA;

-- gold_location_customers
CREATE TABLE IF NOT EXISTS gold_location_customers (
  district STRING,
  neighborhood STRING,
  total_products BIGINT,
  ranking INT,
  longitude DOUBLE,
  latitude DOUBLE
) USING DELTA;

-- bronze_customers
CREATE TABLE IF NOT EXISTS bronze_customers (
  customer_id BIGINT,
  name STRING,
  phone STRING,
  email STRING,
  address STRING
) USING DELTA;

-- bronze_employees
CREATE TABLE IF NOT EXISTS bronze_employees (
  employee_id BIGINT,
  name STRING,
  phone STRING,
  email STRING,
  address STRING,
  comission DOUBLE
) USING DELTA;

-- silver_employees
CREATE TABLE IF NOT EXISTS silver_employees (
  employee_id BIGINT,
  name STRING,
  phone STRING,
  email STRING,
  address STRING,
  comission DOUBLE,
  first_sale TIMESTAMP,
  last_sale TIMESTAMP,
  total_orders BIGINT,
  total_products BIGINT,
  min_quantity_sold INT,
  max_quantity_sold INT,
  load_date TIMESTAMP,
  days_since_first_sale INT
) USING DELTA;

-- silver_customers
CREATE TABLE IF NOT EXISTS silver_customers (
  customer_id BIGINT,
  name STRING,
  phone STRING,
  email STRING,
  address STRING,
  first_purchase TIMESTAMP,
  last_purchase TIMESTAMP,
  total_orders BIGINT,
  total_products BIGINT,
  min_quantity_sold INT,
  max_quantity_sold INT,
  load_date TIMESTAMP,
  days_as_customer INT
) USING DELTA;

-- bronze_geodata
CREATE TABLE IF NOT EXISTS bronze_geodata (
  OBJECTID BIGINT,
  CODIGO STRING,
  NOMBRE STRING,
  IDENTIFICACION STRING,
  LIMITEMUNICIPIOID STRING,
  SUBTIPO_COMUNACORREGIMIENTO BIGINT,
  LINK_DOCUMENTO INT,
  SHAPEAREA DOUBLE,
  SHAPELEN DOUBLE,
  geometry BINARY
) USING DELTA;

-- bronze_medellin
CREATE TABLE IF NOT EXISTS bronze_medellin (
  DPTOMPIO STRING,
  DPTO_CCDGO STRING,
  MPIO_CCDGO STRING,
  MPIO_CNMBR STRING,
  MPIO_CCNCT STRING,
  geometry BINARY
) USING DELTA;

-- gold_inconsistencies_report
CREATE TABLE IF NOT EXISTS gold_inconsistencies_report (
  approved_date DATE,
  total_orders BIGINT,
  total_error_orders BIGINT,
  percent_errors DOUBLE
) USING DELTA;

-- gold_train_model_dataset
CREATE TABLE IF NOT EXISTS gold_train_model_dataset (
  neighborhood STRING,
  event_date DATE,
  quantity_products BIGINT,
  day_number INT,
  demand_lag_1 BIGINT,
  demand_lag_2 BIGINT,
  avg_demand_2d DOUBLE
) USING DELTA;

-- gold_demand_prediction
CREATE TABLE IF NOT EXISTS gold_demand_prediction (
  neighborhood STRING,
  prediction DOUBLE,
  avg_bias DOUBLE,
  bias_percentage DOUBLE
) USING DELTA;
