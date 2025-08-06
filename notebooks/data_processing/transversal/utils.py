# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Explicación del funcionamiento de este notebook
# MAGIC
# MAGIC Este notebook está diseñado para guiar al usuario a través de un flujo de trabajo específico, utilizando celdas de código y markdown para documentar y ejecutar procesos. Aquí se explican los objetivos, pasos principales y consideraciones importantes para su uso. Cada sección del notebook contiene instrucciones claras y ejemplos prácticos para facilitar la comprensión y el aprendizaje.

# COMMAND ----------

from shapely import wkb
import geopandas as gpd

def spark_to_geopandas(df_spark, crs="EPSG:4326"):

    df = df_spark.toPandas()

    if isinstance(df['geometry'].iloc[0], bytes):
        df['geometry'] = df['geometry'].apply(wkb.loads)

    gdf = gpd.GeoDataFrame(df, geometry='geometry')

    if gdf.crs is None:
        gdf.set_crs(crs, inplace=True)
        
    return gdf

# COMMAND ----------

import pytz

horario_colombia = pytz.timezone("America/Bogota")

# COMMAND ----------

from pyspark.sql.functions import col, avg, round

def calculate_bias_by_neighborhood(dataset):

    bias_by_neighborhood = dataset.groupBy("neighborhood").agg(
        avg("quantity_products").alias("avg_real"),
        avg("prediction").alias("avg_predicted")
    ).withColumn(
        "avg_bias", col("avg_predicted") - col("avg_real")
    ).withColumn(
        "bias_percentage", round((col("avg_bias") / col("avg_real")), 3)
    ).orderBy("neighborhood")

    return bias_by_neighborhood