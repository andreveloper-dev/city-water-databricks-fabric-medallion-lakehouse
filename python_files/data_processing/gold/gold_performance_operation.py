# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Explicación de este Notebook
# MAGIC
# MAGIC Este notebook está diseñado para proporcionar una visión general y explicación de los conceptos, procesos y resultados presentados. Aquí encontrarás descripciones detalladas de cada sección, así como el propósito y los objetivos del análisis realizado. Utiliza este documento como guía para comprender el flujo de trabajo y la lógica detrás de cada paso implementado en el notebook.

# COMMAND ----------

# MAGIC %run ../Transversal/config

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, to_date, col


def data_preprocessing(table_silver):
    
    df = spark.table(table_silver)

    df = df.dropDuplicates(["order_id"])

    df = df.withColumn("approved_date", to_timestamp(col("event_date"), "dd/MM/yyyy HH:mm:ss"))
    df = df.withColumn("approved_date", to_date(col("approved_date")))

    return df


silver_stream = data_preprocessing(table_silver=Silver_Orders)

# COMMAND ----------

from pyspark.sql.functions import col, count, sum as _sum, round


def performace_operation_batch(table_silver, table_gold):

    silver_stream_agg = table_silver.groupBy("approved_date").agg(
    count("order_id").alias("total_orders"),
    _sum("quantity_products").alias("total_products"))

    silver_stream_agg = silver_stream_agg.withColumn(
        "avg_products_per_order",
        round(col("total_products") / col("total_orders"), 2))

    silver_stream_agg.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(table_gold)

    return silver_stream_agg

# COMMAND ----------

from pyspark.sql.functions import when


def inconsistencies_report(table_silver, table_gold):

    silver_stream_agg = table_silver.groupBy("approved_date").agg(count("order_id").alias("total_orders"))

    error_silver_stream = table_silver.filter((col("district") == "None") | (col("district") == "DESCONOCIDA"))
    error_silver_agg = error_silver_stream.groupBy("approved_date").agg(count("order_id").alias("total_error_orders"))

    df_merged = silver_stream_agg.join(
    error_silver_agg,
    on="approved_date",
    how="left"
    )

    df_final = df_merged.na.fill({"total_error_orders": 0})

    df_final = df_final.withColumn(
        "percent_errors",
        round(when(col("total_orders") != 0,
                (col("total_error_orders") / col("total_orders"))
            ).otherwise(0), 3))

    df_final.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(table_gold)

    return df_final 

# COMMAND ----------

performace_operation_batch(
    table_silver=silver_stream,
    table_gold=Gold_Performance_Operations
)

inconsistencies_report(
    table_silver=silver_stream,
    table_gold=Gold_Inconsistencies_Report
)