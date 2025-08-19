# Databricks notebook source
# MAGIC %run "../includes/common functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

race_results_df = spark.table("f1_presentation.race_results")

# COMMAND ----------

from pyspark.sql.functions import sum, count, when, col, count

# COMMAND ----------

constructor_standings_df=race_results_df.groupBy("race_year", "team").agg(sum("points").alias("total_points"), count(when(col("position")==1, True)).alias("wins"))


# COMMAND ----------

display(constructor_standings_df.filter("race_year=2020"))


# COMMAND ----------

from pyspark.sql.functions import desc, rank, asc
from pyspark.sql.window import Window

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df = constructor_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df.filter("race_year=2020"))

# COMMAND ----------

merge_condition = "tgt.team = src.team AND tgt.race_year = src.race_year"
merge_delta_data(final_df, 'f1_presentation', 'constructor_standings', merge_condition, 'race_year')