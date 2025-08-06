# 💧 UNALWater - Medallion Architecture in Databricks

## 1. Introduction

**Project Description:**  
This project implements a modern Big Data architecture in **Databricks** using the **Medallion Architecture** (Bronze, Silver, Gold). It simulates water bottle sales events across Medellín to identify neighborhoods with the highest demand, aiming to optimize the marketing strategy of the fictional company **UNALWater**.

**Objectives:**
- Design a scalable solution for ingesting and analyzing simulated real-time sales data.
- Implement a complete data pipeline from raw ingestion to analytical dashboards.
- Generate valuable business insights through visualizations and analytical models.
- Apply data engineering best practices in a realistic Big Data environment.

---

## 2. Project Structure

```bash
/city-water-databricks-medallion-lakehouse/
├── notebooks/
│   ├── data_processing/
│   │   ├── bronze/
│   │   │   └── bronze_streaming.py
│   │   ├── silver/
│   │   │   ├── silver_customers.py
│   │   │   ├── silver_employees.py
│   │   │   └── silver_streaming.py
│   │   ├── gold/
│   │   │   ├── gold_customers.py
│   │   │   ├── gold_employees.py
│   │   │   └── gold_performance_operation.py
│   │   └── transversal/
│   │       ├── clean_directories_and_tables.py
│   │       ├── config.py
│   │       ├── data_simulator.py
│   │       ├── load_data.py
│   │       └── utils.py
│   └── models/
│       ├── data_processing.py
│       ├── model_training.py
│       └── demand_prediction.py
├── data/
│   ├── 50001.parquet
│   ├── bronze.snappy.parquet
│   ├── silver.snappy.parquet
│   ├── medellin_neighborhoods.parquet
│   ├── customers.parquet
│   └── employees.parquet
├── volumes/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── diagrams/
│   └── architecture_diagram.png
└── README.md
```

**Component Description:**
- `notebooks/`: Databricks notebooks for each stage of the data pipeline.
- `utils/`: Reusable helper modules (e.g., polygon mapping).
- `data/`: Simulated data and geojson files for geospatial analysis.
- `volumes/`: Stores data in Bronze, Silver, and Gold layers.
- `diagrams/`: Visual architecture diagrams.
- `README.md`: Project documentation.

---

## 3. Pipelines

**Stages:**
- **Event Simulation**: Generate synthetic sales events every 30 seconds.
- **Bronze Ingestion**: Store raw data in Delta Lake using Spark Structured Streaming.
- **Silver Transformation**: Clean and enrich with geospatial context.
- **Gold Aggregation**: Compute KPIs and summary tables.
- **Visualization**: Explore results using Databricks SQL dashboards.

**Pipeline Flow Diagram:**

```
[ Simulated JSON Events ]
          ↓
[ Bronze Layer - Raw ]
          ↓
[ Silver Layer - Clean + Geo ]
          ↓
[ Gold Layer - Metrics ]
          ↓
[ Dashboards & Analysis ]
```

**Stage Details:**
- **Simulation**: Python script generates sale records with metadata and coordinates.
- **Bronze**: Real-time ingestion and Delta Lake storage.
- **Silver**: Parse dates and enrich with spatial joins (GeoJSON).
- **Gold**: Metrics by district/time and export to SQL tables.
- **Visualization**: Dashboards with KPIs, maps, and interactive widgets.

**Technologies Used:**
- Apache Spark (Structured Streaming)
- Delta Lake
- Python
- SQL (Databricks)
- GeoJSON + GeoSpark or `ST_Contains`
- Databricks SQL Widgets

---

## 4. Databases

**Schemas:**
- `bronze_db`: Raw ingested data
- `silver_db`: Clean and enriched data
- `gold_db`: Aggregated analytical data

**Key Queries:**
- Top neighborhoods by product sales.
- Aggregation by hour/day of week.
- Dashboard filters using SQL widgets.

---

## 5. Setup & Deployment

**Environment Setup:**
- Databricks cluster with Runtime 12+ and Spark 3.x
- Upload scripts and GeoJSON files
- Create volumes in Databricks UI
- Configure SQL widgets and permissions (if using dashboards)

**Deployment Steps:**
1. Run `01_data_simulation.py`
2. Execute pipeline in order:
   - `02_bronze_ingestion.py`
   - `03_silver_processing.py`
   - `04_gold_analytics.py`
3. Visualize with `05_dashboard_visualization.sql`

**Requirements:**
- `geopandas` or `pyspark.sql.functions` with geospatial extensions
- Access to Databricks Volumes
- GeoJSON data for Medellín districts

---

## 6. Maintenance & Updates

**Maintenance:**
- Ensure streaming jobs are active
- Monitor checkpoints and lag via Spark UI
- Check for null/inconsistent values in Silver layer

**Updates:**
- Enhance simulator for new data patterns
- Retrain models if consumption behavior changes
- Add new KPIs and visualizations

---

## 7. References

- [Databricks Structured Streaming](https://docs.databricks.com/)
- [Medallion Architecture Overview](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake](https://delta.io/)
- [Geospatial joins in Spark](https://databricks.com/blog/2021/09/27/performing-geospatial-joins-in-apache-spark.html)
