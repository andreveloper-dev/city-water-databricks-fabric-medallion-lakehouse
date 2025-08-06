# Databricks notebook source
# MAGIC %md
# MAGIC # Explicación del Notebook
# MAGIC
# MAGIC Este notebook está diseñado para procesar y analizar datos de pedidos utilizando Databricks y Apache Spark.
# MAGIC
# MAGIC ## 1. Ingesta de Datos en Streaming (Bronze Layer)
# MAGIC
# MAGIC Se define la función `bronze_stream` para leer datos en formato JSON desde una zona de aterrizaje (landing zone) usando Spark Structured Streaming. El esquema de los datos incluye información geográfica, fecha, identificadores de cliente y empleado, cantidad de productos y el identificador del pedido.
# MAGIC
# MAGIC - **Eliminación de Duplicados:** Se eliminan duplicados basados en `order_id`.
# MAGIC - **Escritura en Delta Lake:** Los datos limpios se escriben en una tabla Delta, permitiendo consultas eficientes y manejo de grandes volúmenes de datos.
# MAGIC - **Checkpointing:** Se utiliza una ubicación de checkpoint para tolerancia a fallos y reinicio seguro del stream.
# MAGIC
# MAGIC ## 2. Ejecución del Stream
# MAGIC
# MAGIC Se ejecuta la función `bronze_stream` pasando las rutas y nombres de tabla correspondientes para iniciar el proceso de ingesta y almacenamiento de los datos de pedidos.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC Este flujo permite analizar sentimientos de reseñas y procesar pedidos en tiempo real, facilitando la toma de decisiones basada en datos y la integración de análisis avanzados en pipelines de datos empresariales.
# MAGIC """
# MAGIC
# MAGIC with open("/Workspace/explicacion_notebook.md", "w", encoding="utf-8") as f:
# MAGIC     f.write(notebook_explanation)

# COMMAND ----------

# MAGIC %run ../Transversal/config

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType


def bronze_stream(landing_zone, checkpoint, table):

    schema = StructType() \
        .add("latitude", DoubleType()) \
        .add("longitude", DoubleType()) \
        .add("date", StringType()) \
        .add("customer_id", StringType()) \
        .add("employee_id", StringType()) \
        .add("quantity_products", IntegerType()) \
        .add("order_id", StringType())

    raw_stream_df = (
        spark.readStream
            .format("json")
            .schema(schema)
            .option("path", landing_zone)
            .load()
    )

    clean_stream_df = raw_stream_df.dropDuplicates(["order_id"])  # Debe cambiarse a watermark y window cuando sean muchos registros.

    query = (
        clean_stream_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(once=True)
            .option("mergeSchema", "true")
            .toTable(table))

    return query


query = bronze_stream(
            landing_zone=landing_zone_path,
            checkpoint=checkpoint_bronze,
            table=Bronze_Orders)