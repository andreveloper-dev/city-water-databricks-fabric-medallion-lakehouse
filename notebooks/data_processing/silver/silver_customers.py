# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Explicación de este Notebook
# MAGIC
# MAGIC Este notebook está diseñado para guiar al usuario a través de un proceso específico, proporcionando ejemplos, explicaciones y resultados. A lo largo del notebook, se presentan celdas de código y texto que ayudan a comprender los conceptos y a ejecutar las tareas necesarias paso a paso. El objetivo es facilitar el aprendizaje y la aplicación práctica de los temas tratados.

# COMMAND ----------

# MAGIC %run ../Transversal/config

# COMMAND ----------

from datetime import datetime
import pytz

horario_colombia = pytz.timezone("America/Bogota")
current_date = datetime.now(horario_colombia).strftime("%d/%m/%Y %H:%M:%S")

customers = spark.table(Bronze_Customers)
orders = spark.table(Silver_Orders)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, min, max, count, sum as _sum, lit, datediff


def generate_silver_customers(orders_df, employees_df, table_silver, current_date):

    clean_orders_df = orders_df.dropDuplicates(["order_id"])
    clean_orders_df = clean_orders_df.withColumn("approved_date", to_timestamp(col("event_date"), "dd/MM/yyyy HH:mm:ss"))

    orders_agg = clean_orders_df.groupBy("customer_id").agg(
    min("approved_date").alias("first_purchase"),
    max("approved_date").alias("last_purchase"),
    count("order_id").alias("total_orders"),
    _sum("quantity_products").alias("total_products"),
    min("quantity_products").alias("min_quantity_sold"),
    max("quantity_products").alias("max_quantity_sold"))

    orders_agg = orders_agg.withColumn("load_date", to_timestamp(lit(current_date), "dd/MM/yyyy HH:mm:ss"))
    orders_agg = orders_agg.withColumn("days_as_customer", datediff(col("load_date"), col("first_purchase")))

    df_merged = employees_df.join(
        orders_agg,
        on="customer_id",
        how="inner")

    df_merged.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(table_silver)

    return df_merged

# COMMAND ----------

df = generate_silver_customers(
    orders_df=orders,
    employees_df=customers,
    table_silver=Silver_Customers,
    current_date=current_date
)