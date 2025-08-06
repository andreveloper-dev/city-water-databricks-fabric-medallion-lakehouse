# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Explicación del Funcionamiento de este Notebook
# MAGIC
# MAGIC Este notebook está diseñado para guiar al usuario a través de una serie de pasos o análisis específicos. A continuación se describen sus principales funcionalidades:
# MAGIC
# MAGIC - **Importación de librerías:** Se cargan las bibliotecas necesarias para el análisis de datos y visualización.
# MAGIC - **Carga de datos:** Se importan los datos que serán analizados a lo largo del notebook.
# MAGIC - **Procesamiento de datos:** Se realizan tareas de limpieza, transformación y preparación de los datos para su análisis.
# MAGIC - **Análisis exploratorio:** Se exploran los datos mediante estadísticas descriptivas y visualizaciones.
# MAGIC - **Modelado o análisis avanzado:** Se aplican técnicas de modelado, machine learning o análisis avanzado según el objetivo del notebook.
# MAGIC - **Conclusiones:** Se resumen los hallazgos y se presentan las conclusiones principales.
# MAGIC
# MAGIC Este documento sirve como guía y referencia para entender cada paso realizado en el notebook.

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

files = spark.read.format("binaryFile").load(landing_zone_path).select("path")

for row in files.toLocalIterator():
    dbutils.fs.rm(row.path, recurse=True)


# COMMAND ----------

def remove_checkpoints(folder_path):
    items = dbutils.fs.ls(folder_path)

    for item in items:
        dbutils.fs.rm(item.path, recurse=True)

remove_checkpoints(folder_path=checkpoint_bronze)
remove_checkpoints(folder_path=checkpoint_silver)


# COMMAND ----------

tablas = [Bronze_Orders, Silver_Orders, Gold_Inconsistencies_Report, Gold_Location_Customers, Gold_Location_Employees, Gold_Performance_Operations, Gold_Performance_Employees, Gold_Train_Model_Dataset, 'gold_demand_prediction', Bronze_Customers, Bronze_Employes, Bronze_Geodata, Bronze_Medellin, Silver_Customers, Silver_Employees]

for tabla in tablas:
    print(f"DELETE FROM `unalwater_v2`.`default`.`{tabla}`")
    spark.sql(f"DELETE FROM `unalwater_v2`.`default`.`{tabla}`")