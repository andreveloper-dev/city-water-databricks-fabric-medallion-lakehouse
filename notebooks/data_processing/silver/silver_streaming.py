# Databricks notebook source
# MAGIC %md
# MAGIC # Explicación del Notebook
# MAGIC
# MAGIC Este notebook realiza procesamiento de datos geoespaciales y de pedidos utilizando PySpark y Delta Lake en Databricks.
# MAGIC
# MAGIC ## 1. Procesamiento de Geodatos
# MAGIC - Se carga una tabla de datos geográficos (`Bronze_Geodata`) y se convierte a un GeoDataFrame.
# MAGIC - Se extraen los polígonos de barrios y comunas y se transmiten como variable de broadcast para eficiencia.
# MAGIC - Se define una función UDF (`get_comuna_barrio`) que, dado una latitud y longitud, determina a qué comuna y barrio pertenece el punto geográfico.
# MAGIC
# MAGIC ## 2. Procesamiento de Pedidos (Streaming)
# MAGIC - Se define la función `silver_stream` que lee datos de pedidos en streaming desde una tabla Delta (`table_bronze`).
# MAGIC - Se eliminan duplicados por `order_id`.
# MAGIC - Se convierte la columna de fecha a timestamp y se enriquece cada pedido con la información geográfica de comuna y barrio usando la UDF definida.
# MAGIC - Se seleccionan y transforman columnas relevantes, incluyendo descomposición de la fecha en año, mes, día, hora, minuto y segundo.
# MAGIC - Finalmente, los datos enriquecidos se escriben en una tabla Delta (`table_silver`) en modo append, con control de versiones y tolerancia a cambios de esquema.
# MAGIC
# MAGIC Este flujo permite mantener una tabla Silver de pedidos enriquecida con información geográfica, lista para análisis posteriores.

# COMMAND ----------

# MAGIC %run ../Transversal/config

# COMMAND ----------

# MAGIC %run ../Transversal/utils

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from shapely.geometry import Point, shape

neighborhoods_spark_df = spark.table(Bronze_Geodata)
gdf_neighborhoods = spark_to_geopandas(df_spark=neighborhoods_spark_df)


polygons = [
    (row['IDENTIFICACION'], row['NOMBRE'], shape(row['geometry']))
    for _, row in gdf_neighborhoods.iterrows()
]

broadcast_polygons = spark.sparkContext.broadcast(polygons)

def get_comuna_barrio(lat, lon):

    point = Point(float(lon), float(lat))

    for comuna, barrio, poly in broadcast_polygons.value:
        if poly.contains(point):
            return (str(comuna), str(barrio))
            
    return ('DESCONOCIDA', 'DESCONOCIDO')


schema = StructType([
    StructField("comuna", StringType(), True),
    StructField("barrio", StringType(), True),
])

comuna_barrio_udf = udf(get_comuna_barrio, schema)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, date_format, year, month, dayofmonth, hour, minute, second
import time


def silver_stream(table_bronze, checkpoint, table_silver):

    bronze_stream = (
        spark.readStream
            .format("delta")
            .table(table_bronze)
    )

    clean_stream_df = bronze_stream.dropDuplicates(["order_id"])

    df_with_timestamp = clean_stream_df.withColumn('event_timestamp', to_timestamp(col('date'), 'dd/MM/yyyy HH:mm:ss'))
    df_with_geodata  = df_with_timestamp.withColumn("location_info", comuna_barrio_udf(col("latitude"), col("longitude")))\
                                        .withColumn("comuna", col("location_info.comuna"))\
                                        .withColumn("barrio", col("location_info.barrio"))\
                                        .drop("location_info")

    silver_df = df_with_geodata.select(
        date_format(col("event_timestamp"), "ddMMyyyy").alias("partition_date"),
        col("order_id"),
        col("customer_id"),
        col("employee_id"),
        col("quantity_products"),
        col("latitude"),
        col("longitude"),
        col("comuna").cast(StringType()).alias("district"),
        col("barrio").cast(StringType()).alias("neighborhood"),
        col("date").alias("event_date"),
        year(col("event_timestamp")).cast(IntegerType()).alias("event_year"),
        month(col("event_timestamp")).cast(IntegerType()).alias("event_month"),
        dayofmonth(col("event_timestamp")).cast(IntegerType()).alias("event_day"),
        hour(col("event_timestamp")).cast(IntegerType()).alias("event_hour"),
        minute(col("event_timestamp")).cast(IntegerType()).alias("event_minute"),
        second(col("event_timestamp")).cast(IntegerType()).alias("event_second")
    )

    query = (
        silver_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("mergeSchema", "true")
            .trigger(once=True)
            .toTable(table_silver)
    )

    return query



query = silver_stream(
        table_bronze=Bronze_Orders,
        checkpoint=checkpoint_silver,
        table_silver=Silver_Orders
        )