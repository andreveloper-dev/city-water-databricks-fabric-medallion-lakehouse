-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Explicación del Funcionamiento de este Notebook
-- MAGIC
-- MAGIC Este notebook está diseñado para guiar al usuario a través de un flujo de trabajo específico, utilizando herramientas y funciones de Databricks. A lo largo del notebook, se presentan celdas de código y markdown que explican cada paso del proceso, permitiendo la ejecución interactiva y la documentación del análisis realizado.
-- MAGIC
-- MAGIC El objetivo principal es facilitar la comprensión y reproducibilidad de los análisis, mostrando tanto la lógica detrás de cada operación como los resultados obtenidos. Además, se incluyen comentarios y descripciones para aclarar el propósito de cada sección y cómo se relaciona con el flujo general del trabajo.
-- MAGIC
-- MAGIC Al finalizar, el usuario tendrá una visión clara de los datos procesados, las transformaciones aplicadas y los resultados obtenidos, todo documentado de manera estructurada y accesible en formato markdown.

-- COMMAND ----------

CREATE VOLUME IF NOT EXISTS unalwater_v2.default.checkpoint_BRONZE
CREATE VOLUME IF NOT EXISTS unalwater_v2.default.checkpoint_SILVER
CREATE VOLUME IF NOT EXISTS unalwater_v2.default.Input
CREATE VOLUME IF NOT EXISTS unalwater_v2.default.landing_zone
CREATE VOLUME IF NOT EXISTS unalwater_v2.default.tmp