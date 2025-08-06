# City Water Project: Databricks-Medallion-Lakehouse
Implementation of a Medallion Architecture in Databricks for a fictional water distribution company. Data is organized into Bronze, Silver, and Gold layers to streamline ingestion, cleansing, and analysis of sales, customer, and consumption data for scalable, data-driven decisions.

# Databricks notebook source
# MAGIC %md
# MAGIC # **Readme**
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Introducción
# MAGIC     - Descripción general del proyecto:
# MAGIC       Este proyecto consiste en el diseño e implementación de una arquitectura Big Data moderna en Databricks utilizando la estrategia Medallion Architecture (Bronze, Silver, Gold). Se simulan eventos de ventas de botellas de agua en la ciudad de Medellín para identificar comunas con mayor demanda, con el fin de optimizar estrategias de marketing de la empresa UNALWater.
# MAGIC
# MAGIC     - Objetivos del proyecto:
# MAGIC       + Diseñar una solución escalable para ingestión y análisis de datos de ventas simuladas en tiempo real.
# MAGIC       + Implementar un flujo completo de datos desde raw data hasta dashboards analíticos.
# MAGIC       + Generar insights valiosos para el negocio mediante visualizaciones y modelos analíticos simples.
# MAGIC       + Practicar buenas prácticas de ingeniería de datos en un entorno de Big Data realista.
# MAGIC
# MAGIC 2. Estructura del Proyecto
# MAGIC     - Descripción de la estructura de directorios y archivos:
# MAGIC         bash
# MAGIC         Copiar
# MAGIC         Editar
# MAGIC         /city-water-databricks-medallion-lakehouse/
# MAGIC         ├── notebooks/
# MAGIC         │   ├── data_processing/
# MAGIC         │   │    ├── bronze/
# MAGIC         │   │       └── bronze_streaming.py
# MAGIC         │   │    ├── silver/
# MAGIC         │   │       ├── silver_customers.py
# MAGIC         │   │       ├── silver_employees.py
# MAGIC         │   │       └── silver_streaming.py
# MAGIC         │   │    ├── gold/
# MAGIC         │   │       ├── gold_customers.py
# MAGIC         │   │       ├── gold_employees.py
# MAGIC         │   │       └── gold_performance_operation.py
# MAGIC         │   │    └── transversal/
# MAGIC         │   │        ├── clean_directories_and_tables.py
# MAGIC         │   │        ├── config.py
# MAGIC         │   │        ├── data_simulator.py
# MAGIC         │   │        ├── load_data.py
# MAGIC         │   │        └── utils.py
# MAGIC         │   └── models/
# MAGIC         │       ├── data_processing.py
# MAGIC         │       ├── model_training.py
# MAGIC         │       └── demand_prediction.py
# MAGIC         ├── data/
# MAGIC         │   ├── 50001.parquet
# MAGIC         │   ├── bronze.snappy.parquet
# MAGIC         │   ├── silver.snappy.parquet
# MAGIC         │   ├── medellin_neighborhoods.parquet
# MAGIC         │   ├── customers.parquet
# MAGIC         │   └── employees.parquet
# MAGIC         ├── volumes/
# MAGIC         │   ├── bronze/
# MAGIC         │   ├── silver/
# MAGIC         │   └── gold/
# MAGIC         ├── diagrams/
# MAGIC         │   └── architecture_diagram.png
# MAGIC         └── README.md
# MAGIC     - Propósito de cada archivo y directorio:
# MAGIC       Elemento	Descripción
# MAGIC       notebooks/	Contiene los notebooks de Databricks que ejecutan cada etapa del pipeline.
# MAGIC       utils/	Módulos auxiliares con funciones reutilizables (como mapeo de polígonos).
# MAGIC       data/	Contiene datos simulados y archivos geojson para análisis geoespacial.
# MAGIC       volumes/	Almacena la información organizada en Bronze, Silver y Gold.
# MAGIC       diagrams/	Diagramas visuales del sistema.
# MAGIC       README.md	Documentación general del proyecto.
# MAGIC
# MAGIC 3. Pipelines
# MAGIC     - Descripción de cada pipeline:
# MAGIC       + Simulación de eventos: Generación de registros de ventas cada 30 segundos.
# MAGIC       + Ingesta a Bronze: Almacenamiento crudo en Delta Lake.
# MAGIC       + Transformación a Silver: Limpieza y enriquecimiento con datos geoespaciales.
# MAGIC       + Agregación Gold: KPIs y resúmenes para análisis.
# MAGIC       + Visualización: Dashboards en Databricks SQL.
# MAGIC
# MAGIC     - Diagrama de flujo de los pipelines:
# MAGIC         css
# MAGIC         Copiar
# MAGIC         Editar
# MAGIC         [ Simulación JSON ]
# MAGIC               ↓
# MAGIC         [ Bronze Layer - Crudo ]
# MAGIC               ↓
# MAGIC         [ Silver Layer - Limpio + Geo ]
# MAGIC               ↓
# MAGIC         [ Gold Layer - Métricas ]
# MAGIC               ↓
# MAGIC         [ Dashboards & Análisis ]
# MAGIC     - Explicación de cada etapa del pipeline:
# MAGIC       + Simulación: Python script que genera datos con coordenadas válidas y metadatos de ventas.
# MAGIC       + Bronze: Ingesta en tiempo real con Spark Structured Streaming y escritura en Delta Lake.
# MAGIC       + Silver:	Parseo de fechas, y cruce de coordenadas con polígonos geojson.
# MAGIC       + Gold: Cálculo de ventas por comuna, por hora, etc. y exportación a tablas SQL.
# MAGIC       + Visualización: Dashboards SQL con KPIs, mapas y widgets interactivos.
# MAGIC
# MAGIC     - Herramientas y tecnologías utilizadas:
# MAGIC       + Apache Spark (Structured Streaming)
# MAGIC       + Delta Lake
# MAGIC       + Python
# MAGIC       + SQL en Databricks
# MAGIC       + GeoJSON + GeoSpark (o ST_Contains si está habilitado)
# MAGIC       + Widgets para dashboards
# MAGIC
# MAGIC 4. Bases de Datos
# MAGIC     - Descripción de las bases de datos utilizadas:
# MAGIC       + Se utilizan tres bases (schemas) correspondientes a las zonas Medallion: bronze_db, silver_db, gold_db
# MAGIC     - Consultas y procedimientos almacenados importantes:
# MAGIC       + Top comunas por cantidad de productos vendidos.
# MAGIC       + Agrupación por hora/día de la semana.
# MAGIC       + Dashboards con filtros (widgets).
# MAGIC 6. Configuración y Despliegue
# MAGIC     - Instrucciones para la configuración del entorno:
# MAGIC       + Instalar clúster de Databricks con entorno Runtime 12+ con Spark 3.x.
# MAGIC       + Subir scripts y archivos geojson.
# MAGIC       + Crear volúmenes con Databricks Volumes UI.
# MAGIC       + Configurar widgets y permisos SQL si se usan dashboards colaborativos.
# MAGIC
# MAGIC     - Pasos para el despliegue del proyecto:
# MAGIC       + Ejecutar notebook 01_data_simulation.py.
# MAGIC       + Ejecutar pipeline en orden:
# MAGIC       + 02_bronze_ingestion.py
# MAGIC       + 03_silver_processing.py
# MAGIC       + 04_gold_analytics.py
# MAGIC       + Visualizar resultados con 05_dashboard_visualization.sql.
# MAGIC
# MAGIC     - Dependencias y requisitos previos:
# MAGIC       + geopandas o pyspark.sql.functions + funciones geoespaciales.
# MAGIC       + Acceso a Volumes.
# MAGIC       + Datos geojson de Medellín (comunas, distritos).
# MAGIC
# MAGIC 7. Mantenimiento y Actualizaciones
# MAGIC     - Procedimientos para el mantenimiento del proyecto:
# MAGIC       + Validar que los jobs de streaming estén activos.
# MAGIC       + Monitorear checkpoints y lag con Spark UI.
# MAGIC       + Validar datos nulos o inconsistentes en Silver.
# MAGIC
# MAGIC     - Instrucciones para actualizar componentes del proyecto:
# MAGIC       + Actualizar simulador para nuevos patrones de datos.
# MAGIC       + Reentrenar modelo analítico si cambia el patrón de consumo.
# MAGIC       + Agregar nuevas visualizaciones con KPIs emergentes.
# MAGIC
# MAGIC 8. Referencias
# MAGIC     - Documentación oficial de Databricks Structured Streaming
# MAGIC     - Arquitectura Medallion: Databricks Medallion Architecture
# MAGIC     - Introducción a Delta Lake: https://delta.io/
# MAGIC     - GeoSpark / ST_Contains en Spark SQL: https://databricks.com/blog/2021/09/27/performing-geospatial-joins-in-apache-spark.html
# MAGIC
# MAGIC
