# Databricks notebook source
# MAGIC %md
# MAGIC # Explicación del Notebook
# MAGIC
# MAGIC Este notebook realiza el análisis de desempeño y localización de empleados a partir de datos de órdenes y empleados almacenados en tablas Delta.
# MAGIC
# MAGIC ## Flujo de trabajo:
# MAGIC
# MAGIC 1. **Carga de datos**  
# MAGIC    Se cargan las tablas `Silver_Orders` y `Silver_Employees` en DataFrames Spark.
# MAGIC
# MAGIC 2. **Cálculo de desempeño de empleados**  
# MAGIC    - Se agregan las órdenes por empleado para obtener el total de productos vendidos.
# MAGIC    - Se calculan los cuartiles de desempeño según la cantidad de productos vendidos.
# MAGIC    - Se asigna un cuartil de desempeño a cada empleado.
# MAGIC    - Se calcula el monto de comisión para cada empleado.
# MAGIC    - El resultado se guarda en la tabla `Gold_Performance_Employees`.
# MAGIC
# MAGIC 3. **Obtención de última localización de empleados**  
# MAGIC    - Se selecciona la última orden de cada empleado para obtener su última ubicación registrada.
# MAGIC    - Se une esta información con los datos personales del empleado.
# MAGIC    - El resultado se guarda en la tabla `Gold_Location_Employees`.
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC El objetivo es generar dos tablas Gold:
# MAGIC - Una con el desempeño y comisiones de los empleados.
# MAGIC - Otra con la última localización conocida de cada empleado.
# MAGIC
# MAGIC Ambas tablas pueden ser utilizadas para análisis posteriores y visualizaciones.

# COMMAND ----------

# MAGIC %run ../Transversal/config

# COMMAND ----------

orders = spark.table(Silver_Orders)

employees = spark.table(Silver_Employees)

# COMMAND ----------

from pyspark.sql.functions import col, sum as _sum, when, round


def performance_employees(df_orders, df_employees, bottle_price, table_gold):

    orders_filtered = df_orders.select('employee_id', 'quantity_products')

    orders_filtered_agg = orders_filtered.groupBy("employee_id").agg(
    _sum("quantity_products").alias("total_products"))

    q1, q2, q3 = orders_filtered_agg.approxQuantile("total_products", [0.25, 0.5, 0.75], 0.1)

    orders_quartile = orders_filtered_agg.withColumn(
        "performance_quartile",
        when(col("total_products") > q3, 1)
        .when(col("total_products") > q2, 2)
        .when(col("total_products") > q1, 3)
        .otherwise(4))


    employees_filtered = df_employees.select('employee_id', 'name', 'comission')

    df_merged = orders_quartile.join(
        employees_filtered,
        on="employee_id",
        how="left")
    

    df_performance_employees = df_merged.withColumn(
        "commission_amount",
        round(col("total_products") * bottle_price * col("comission"), 2)
    ).orderBy(col("performance_quartile").asc(), col("total_products").desc())


    df_performance_employees.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(table_gold)

    return df_performance_employees

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import to_timestamp, row_number


def employees_location(df_orders, df_employees, table_gold):

    orders_filtered = df_orders.select(
        'employee_id',
        'event_date',
        'district',
        'neighborhood',
        'latitude',
        'longitude')

    orders_filtered = orders_filtered.withColumn("last_location", to_timestamp(col("event_date"), "dd/MM/yyyy HH:mm:ss"))

    window_spec = Window.partitionBy("employee_id").orderBy(col("last_location").desc())
    orders_filtered_sorted = orders_filtered.withColumn("row_num", row_number().over(window_spec))

    last_orders = orders_filtered_sorted.filter(col("row_num") == 1).drop("row_num", "event_date")

    employees_filtered = df_employees.select('employee_id', 'name', 'phone', 'email')

    df_employees_location = last_orders.join(
        employees_filtered,
        on="employee_id",
        how="left")

    df_employees_location.write\
    .format("delta")\
    .mode("overwrite")\
    .option("overwriteSchema", "true")\
    .saveAsTable(table_gold)

    return df_employees_location

# COMMAND ----------

performance_employees(
    df_orders=orders,
    df_employees=employees,
    bottle_price=bottle_price,
    table_gold=Gold_Performance_Employees
    )


employees_location(
    df_orders=orders,
    df_employees=employees,
    table_gold=Gold_Location_Employees
    )