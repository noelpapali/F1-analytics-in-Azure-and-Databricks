# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest results file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the json file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DoubleType

# COMMAND ----------

results_schema = StructType([
    StructField("resultId", IntegerType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("grid", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("positionText", StringType(), True),
    StructField("positionOrder", IntegerType(), True),
    StructField("points", FloatType(), True),
    StructField("laps", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True),
    StructField("fastestLap", IntegerType(), True),
    StructField("rank", IntegerType(), True),
    StructField("fastestLapTime", StringType(), True),
    StructField("fastestLapSpeed", DoubleType(), True),
    StructField("statusId", IntegerType(), True)
])
                                  

# COMMAND ----------

results_df = spark.read \
.option("header", True) \
.schema(results_schema) \
.json(f"/mnt/f1datalake2025/raw/{v_file_date}/results.json")


# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####add ingestion date and rename
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

results_renamed_df = results_df \
                     .withColumnRenamed("resultId", "result_id") \
                     .withColumnRenamed("raceId", "race_id") \
                     .withColumnRenamed("driverId", "driver_id") \
                     .withColumnRenamed("constructorId", "constructor_id") \
                     .withColumnRenamed("positionText", "position_text") \
                     .withColumnRenamed("positionOrder", "position_order") \
                     .withColumnRenamed("fastestLap", "fastest_lap") \
                     .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                     .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                     .withColumnRenamed("statusId", "status_id") \
                     .withColumn("ingestion_date", current_timestamp()) \
                     .withColumn("data_source", lit(v_data_source)) \
                     .withColumn("file_date", lit(v_file_date))
display(results_renamed_df)

# COMMAND ----------


results_final_df = results_renamed_df \
                   .drop("status_id")
display(results_final_df)

# COMMAND ----------

#deduplicating
results_deduped_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])


# COMMAND ----------

# MAGIC %md
# MAGIC #####
# MAGIC write the file as parquet to cleaned and processed container

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_deduped_df, 'f1_processed', 'results', merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")