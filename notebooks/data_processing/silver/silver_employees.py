# Databricks notebook source
# MAGIC %md
# MAGIC # Explicación del Notebook
# MAGIC
# MAGIC Este notebook realiza el procesamiento y enriquecimiento de datos de empleados y órdenes de venta utilizando PySpark en Databricks. A continuación se describen los pasos principales:
# MAGIC
# MAGIC 1. **Carga de Datos**:  
# MAGIC    Se cargan las tablas `Bronze_Employes` y `Silver_Orders` desde el catálogo de Spark como DataFrames.
# MAGIC
# MAGIC 2. **Procesamiento de Órdenes**:  
# MAGIC    - Se eliminan duplicados de órdenes usando el campo `order_id`.
# MAGIC    - Se convierte la columna `event_date` a tipo timestamp y se renombra como `approved_date`.
# MAGIC    - Se agrupan las órdenes por `employee_id` para calcular:
# MAGIC      - Fecha de la primera y última venta.
# MAGIC      - Total de órdenes y productos vendidos.
# MAGIC      - Cantidad mínima y máxima de productos vendidos en una orden.
# MAGIC
# MAGIC 3. **Enriquecimiento de Empleados**:  
# MAGIC    - Se une la información agregada de órdenes con los datos de empleados.
# MAGIC    - Se agrega la fecha de carga (`load_date`) y los días desde la primera venta.
# MAGIC
# MAGIC 4. **Almacenamiento**:  
# MAGIC    - El DataFrame resultante se guarda como una tabla Delta en el esquema especificado, sobrescribiendo la tabla existente si aplica.
# MAGIC
# MAGIC Este flujo permite mantener una tabla de empleados enriquecida con métricas de ventas, útil para análisis y reportes posteriores.

# COMMAND ----------

# MAGIC %run ../Transversal/config

# COMMAND ----------

# MAGIC %run ../Transversal/utils

# COMMAND ----------

from datetime import datetime
import pytz

current_date = datetime.now(horario_colombia).strftime("%d/%m/%Y %H:%M:%S")

employees = spark.table(Bronze_Employes)
orders = spark.table(Silver_Orders)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col, min, max, count, sum as _sum, lit, datediff


def generate_silver_employees(orders_df, employees_df, table_silver, current_date):

    clean_orders_df = orders_df.dropDuplicates(["order_id"])
    clean_orders_df = clean_orders_df.withColumn("approved_date", to_timestamp(col("event_date"), "dd/MM/yyyy HH:mm:ss"))

    orders_agg = clean_orders_df.groupBy("employee_id").agg(
    min("approved_date").alias("first_sale"),
    max("approved_date").alias("last_sale"),
    count("order_id").alias("total_orders"),
    _sum("quantity_products").alias("total_products"),
    min("quantity_products").alias("min_quantity_sold"),
    max("quantity_products").alias("max_quantity_sold"))

    orders_agg = orders_agg.withColumn("load_date", to_timestamp(lit(current_date), "dd/MM/yyyy HH:mm:ss"))
    orders_agg = orders_agg.withColumn("days_since_first_sale", datediff(col("load_date"), col("first_sale")))

    df_merged = employees_df.join(
        orders_agg,
        on="employee_id",
        how="left")

    df_merged.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(table_silver)

    return df_merged

# COMMAND ----------

generate_silver_employees(
    orders_df=orders,
    employees_df=employees,
    table_silver=Silver_Employees,
    current_date=current_date
)