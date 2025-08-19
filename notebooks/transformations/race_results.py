# Databricks notebook source
# MAGIC %run "../includes/common functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

drivers_df = spark.table("f1_processed.drivers").withColumnRenamed("number", "driver_number") \
    .withColumnRenamed("name", "driver_name") \
    .withColumnRenamed("dob", "driver_dob") \
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.table("f1_processed.constructors").withColumnRenamed("name", "team")

# COMMAND ----------

circuits_df = spark.table("f1_processed.circuits").withColumnRenamed("location", "circuit_location")

# COMMAND ----------

races_df = spark.table("f1_processed.races") \
    .withColumnRenamed("name", "race_name") \
    .withColumnRenamed("race_timestamp", "race_date")
 

# COMMAND ----------

results_df = spark.table("f1_processed.results") \
    .withColumnRenamed("time", "race_time") 

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
.join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
.join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit
final_df = race_results_df.select("race_year", "race_name", "results.race_id", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points","position").withColumn("created_date", current_timestamp()) \
.withColumn("result_file_date", lit(v_file_date))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, COUNT(1)
# MAGIC FROM f1_presentation.race_results
# MAGIC GROUP BY race_id
# MAGIC ORDER BY race_id Desc;