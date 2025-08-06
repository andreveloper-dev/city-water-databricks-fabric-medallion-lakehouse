# Databricks notebook source
# MAGIC %run ../DataProcesing/Transversal/config

# COMMAND ----------

from pyspark.sql.functions import to_date, col


df  = spark.table(Silver_Orders)

df = df.select(
    to_date("event_date", "dd/MM/yyyy HH:mm:ss").alias("event_date"),
    "neighborhood",
    "quantity_products")

df = df.filter(~col("neighborhood").isin("None", "DESCONOCIDO"))

# COMMAND ----------

from pyspark.sql.functions import sum, dayofweek


df_agg = df.groupBy("neighborhood", "event_date")\
           .agg(sum("quantity_products").alias("quantity_products"))

df_agg = df_agg.withColumn("day_number", dayofweek("event_date"))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, avg, when


windowSpec = Window.partitionBy("neighborhood").orderBy("event_date")
windowSpec_2d = windowSpec.rowsBetween(-2, -1)  # 2 días atrás

df_enrichment = df_agg.withColumn("demand_lag_1", lag("quantity_products", 1).over(windowSpec))\
                      .withColumn("demand_lag_2", lag("quantity_products", 2).over(windowSpec))

df_enrichment = df_enrichment.withColumn("avg_demand_2d", avg("quantity_products").over(windowSpec_2d))

df_enrichment = df_enrichment.fillna({
    "demand_lag_1": 0,
    "demand_lag_2": 0,
    "avg_demand_2d": 0
})

# COMMAND ----------

df_enrichment.write\
             .mode("overwrite")\
             .option("overwriteSchema", "true")\
             .saveAsTable(Gold_Train_Model_Dataset)