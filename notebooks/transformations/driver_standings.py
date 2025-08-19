# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

race_results_df = spark.table("f1_presentation.race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, count

# COMMAND ----------

driver_standings_df=race_results_df.groupBy("race_year", "driver_name", "driver_nationality").agg(sum("points").alias("total_points"), count(when(col("position")==1, True)).alias("wins"))


# COMMAND ----------

display(driver_standings_df.filter("race_year=2020"))


# COMMAND ----------

from pyspark.sql.functions import desc, rank, asc
from pyspark.sql.window import Window

# COMMAND ----------

driver_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year=2020"))

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'driver_standings', merge_condition, 'race_year')

# COMMAND ----------

