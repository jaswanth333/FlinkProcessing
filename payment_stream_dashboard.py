#!/usr/bin/env python3

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings,
    DataTypes
)
from pyflink.table.udf import udf


# ----------------- UDF -----------------

brands = {
    'apple': 'Apple',
    'samsung': 'Samsung',
    'oneplus': 'OnePlus',
    'mi': 'Xiaomi',
    'boat': 'boAt',
    'sony': 'Sony'
}

@udf(input_types=[DataTypes.STRING()], result_type=DataTypes.STRING())
def brand_to_name(brand):
    if brand is None:
        return 'Other'
    return brands.get(brand.lower(), brand)

# ----------------- Flink environments -----------------

env = StreamExecutionEnvironment.get_execution_environment()

settings = (
    EnvironmentSettings
    .new_instance()
    .in_streaming_mode()
    .build()
)

t_env = StreamTableEnvironment.create(env, environment_settings=settings)
t_env.get_config().get_configuration().set_boolean(
    "python.fn-execution.memory.managed", True
)

# ----------------- Kafka source (matches main.py) -----------------

t_env.execute_sql("""
CREATE TABLE kafka_transactions (
    transactionId STRING,
    productId STRING,
    productName STRING,
    productCategory STRING,
    productPrice DOUBLE,
    productQuantity INT,
    productBrand STRING,
    currency STRING,
    customerId STRING,
    transactionDate STRING,
    paymentMethod STRING,
    totalAmount DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'financial_transactions',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'dashboard-group',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
)
""")

# ----------------- Elasticsearch sink (per-event for Kibana) -----------------

t_env.execute_sql("""
CREATE TABLE es_dashboard (
    brand STRING,
    category STRING,
    currency STRING,
    total_amount DOUBLE,
    txn_count BIGINT,
    avg_amount DOUBLE,
    txn_hour TIMESTAMP(3)
) WITH (
    'connector' = 'elasticsearch-7',
    'hosts' = 'http://elasticsearch:9200',
    'index' = 'payment_dashboard',
    'format' = 'json'
)
""")

# ----------------- Postgres JDBC sink -----------------

t_env.execute_sql("""
CREATE TABLE pg_transactions (
    transaction_id   STRING,
    product_id       STRING,
    product_name     STRING,
    product_category STRING,
    product_price    DOUBLE,
    product_quantity INT,
    product_brand    STRING,
    currency         STRING,
    customer_id      STRING,
    transaction_date TIMESTAMP(3),
    payment_method   STRING,
    total_amount     DOUBLE,
    PRIMARY KEY (transaction_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/postgres',
    'table-name' = 'payment_transactions',
    'username' = 'postgres',
    'password' = 'postgres',
    'driver' = 'org.postgresql.Driver'
)
""")

# ----------------- Register UDF -----------------

t_env.create_temporary_system_function("brand_to_name", brand_to_name)

# ----------------- Sinks -----------------

# 1) Per-event docs to ES (for Kibana dashboards)
t_env.execute_sql("""
INSERT INTO es_dashboard
SELECT
    brand_to_name(productBrand) AS brand,
    productCategory AS category,
    currency,
    totalAmount AS total_amount,
    1 AS txn_count,
    CAST(totalAmount AS DOUBLE) AS avg_amount,
    CURRENT_TIMESTAMP AS txn_hour
FROM kafka_transactions
""")

# # Postgres JDBC sink (localhost:5432, default postgres schema)
# t_env.execute_sql("""
# CREATE TABLE payment_transactions (
#     transaction_id   STRING PRIMARY KEY NOT ENFORCED,
#     product_id       STRING,
#     product_name     STRING,
#     product_category STRING,
#     product_price    DOUBLE,
#     product_quantity INT,
#     product_brand    STRING,
#     currency         STRING,
#     customer_id      STRING,
#     transaction_date TIMESTAMP(3),
#     payment_method   STRING,
#     total_amount     DOUBLE
# ) WITH (
#     'connector' = 'jdbc',
#     'url' = 'jdbc:postgresql://localhost:5432/postgres',
#     'table-name' = 'payment_transactions',
#     'username' = 'postgres',
#     'password' = 'postgres',  -- Use env vars/secrets in prod
#     'driver' = 'org.postgresql.Driver',
#     'sink.buffer-flush.max-rows' = '1000',
#     'sink.buffer-flush.interval' = '1s',
#     'sink.max-retries' = '3',
#     'sink.parallelism' = '2'
# )
# """)

# # Raw insert from Kafka source
# t_env.execute_sql("""
# INSERT INTO payment_transactions
# SELECT
#     transactionId            AS transaction_id,
#     productId                AS product_id,
#     productName              AS product_name,
#     productCategory          AS product_category,
#     productPrice             AS product_price,
#     productQuantity          AS product_quantity,
#     productBrand             AS product_brand,
#     currency                 AS currency,
#     customerId               AS customer_id,
#     TO_TIMESTAMP(transactionDate) AS transaction_date,
#     paymentMethod            AS payment_method,
#     totalAmount              AS total_amount
# FROM kafka_transactions
# """)
