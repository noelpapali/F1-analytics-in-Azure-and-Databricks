# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest pitstops file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the multi line json file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstops_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("stop", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])
                               

# COMMAND ----------

pitstops_df = spark.read \
.option("multiLine", True) \
.schema(pitstops_schema) \
.json("/mnt/f1datalake2025/raw/pit_stops.json")


# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####add ingestion date and  rename
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstops_final_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ######
# MAGIC write the file as parquet to cleaned and processed container

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(pitstops_final_df, 'f1_processed', 'pitstops', merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pitstops;