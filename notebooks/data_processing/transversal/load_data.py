# Databricks notebook source
# MAGIC %md
# MAGIC ## Este notebook realiza las siguientes tareas:
# MAGIC
# MAGIC 1. Carga datos desde archivos Parquet ubicados en el sistema de archivos DBFS de Databricks. Los archivos corresponden a clientes, empleados, datos geográficos de barrios de Medellín y datos específicos de la ciudad.
# MAGIC 2. Cada archivo se lee en un DataFrame de Spark.
# MAGIC 3. Los DataFrames resultantes se escriben como tablas en el entorno de Databricks, utilizando el modo 'overwrite' para reemplazar cualquier tabla existente con el mismo nombre. Esto permite que los datos estén disponibles para consultas SQL y análisis posteriores.

# COMMAND ----------

# MAGIC %run ../Transversal/config

# COMMAND ----------

customers = spark.read.parquet('dbfs:/Volumes/unalwater_v2/default/files/customers.parquet')
employees = spark.read.parquet('dbfs:/Volumes/unalwater_v2/default/files/employees.parquet')
geodata = spark.read.parquet('dbfs:/Volumes/unalwater_v2/default/files/medellin_neighborhoods.parquet')
medellin = spark.read.parquet('dbfs:/Volumes/unalwater_v2/default/files/50001.parquet')

# COMMAND ----------

customers.write.mode('overwrite').saveAsTable(Bronze_Customers)
employees.write.mode('overwrite').saveAsTable(Bronze_Employes)
geodata.write.mode('overwrite').saveAsTable(Bronze_Geodata)
medellin.write.mode('overwrite').saveAsTable(Bronze_Medellin)