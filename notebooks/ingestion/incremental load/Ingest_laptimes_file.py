# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest laptimes file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the multiple csv files using the spark dataframe reader

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

laptimes_schema = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])
                               

# COMMAND ----------

laptimes_df = spark.read \
.schema(laptimes_schema) \
.csv("/mnt/f1datalake2025/raw/lap_times")


# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

laptimes_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #####add ingestion date and  rename
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

laptimes_final_df = laptimes_df.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ######
# MAGIC write the file as parquet to cleaned and processed container

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(laptimes_final_df, 'f1_processed', 'laptimes', merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")