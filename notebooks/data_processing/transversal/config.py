# Databricks notebook source
# MAGIC %md
# MAGIC # Explicación del Funcionamiento de este Notebook
# MAGIC
# MAGIC Este notebook está diseñado para guiar al usuario a través de un flujo de trabajo específico, que puede incluir la carga de datos, el análisis exploratorio, la visualización y la modelización. A continuación se describen los pasos generales que suele seguir este tipo de notebook:
# MAGIC
# MAGIC 1. **Importación de Librerías**: Se importan las librerías necesarias para el análisis de datos, visualización y modelado.
# MAGIC
# MAGIC 2. **Carga de Datos**: Se cargan los datos desde una fuente externa (por ejemplo, archivos CSV, bases de datos, etc.).
# MAGIC
# MAGIC 3. **Exploración de Datos**: Se realiza un análisis exploratorio para entender la estructura y las características principales de los datos.
# MAGIC
# MAGIC 4. **Limpieza y Preparación de Datos**: Se procesan los datos para corregir errores, manejar valores faltantes y preparar el conjunto de datos para el análisis.
# MAGIC
# MAGIC 5. **Análisis y Visualización**: Se generan gráficos y estadísticas descriptivas para obtener insights relevantes.
# MAGIC
# MAGIC 6. **Modelado**: Si corresponde, se entrena y evalúa un modelo predictivo o de clasificación.
# MAGIC
# MAGIC 7. **Conclusiones**: Se resumen los hallazgos y se proponen posibles pasos siguientes.
# MAGIC
# MAGIC Este flujo puede variar según el objetivo del notebook, pero generalmente sigue esta estructura para facilitar la comprensión y el análisis de los datos.

# COMMAND ----------

Customers = "Files/Input/customers.parquet"
Employees = "Files/Input/employees.parquet"
Geodata = "Files/Input/medellin_neighborhoods.parquet"
Medellin = "Files/Input/50001.parquet"

# COMMAND ----------

Bronze_Customers = "bronze_customers"
Bronze_Employes = "bronze_employees"
Bronze_Medellin = "bronze_medellin"
Bronze_Geodata = "bronze_geodata"
Bronze_Orders = "bronze_orders"

Silver_Orders = "silver_orders"
Silver_Customers = "silver_customers"
Silver_Employees = "silver_employees"

Gold_Location_Customers = "gold_location_customers"
Gold_Performance_Employees = "gold_performance_employees"
Gold_Location_Employees = "gold_location_employees"
Gold_Performance_Operations = "gold_performace_operation"
Gold_Inconsistencies_Report = "gold_inconsistencies_report"
Gold_Train_Model_Dataset = "gold_train_model_dataset"
Gold_Bias_Model = "gold_bias_model"


# Rutas adaptadas a Databricks DBFS (no requieren configuración extra)
gold_train_model_dataset_path = "/Volumes/unalwater_v2/default/files/model/"
landing_zone_path = "/Volumes/unalwater_v2/default/landing_zone"
checkpoint_bronze = "/Volumes/unalwater_v2/default/checkpoint_BRONZE"
checkpoint_silver = "/Volumes/unalwater_v2/default/checkpoint_SILVER"
Gold_Demand_Prediction = "/Volumes/unalwater_v2/default/files/gold_demand_prediction/"


# COMMAND ----------

events = 50             #Cantidad de ordenes generadas por ejecución.
intervalo = 2           #Invervalo en segundos en el que se genera cada orden.

bottle_price = 2000