-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_presentation;

-- COMMAND ----------

-- MAGIC %run "../includes/config"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.parquet(f"{presentation_folder_path}/constructor_standings")
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_presentation.constructor_standings")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.parquet(f"{presentation_folder_path}/driver_standings")
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_presentation.driver_standings")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC df.write.mode("overwrite").saveAsTable("f1_presentation.race_results")