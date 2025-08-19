# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest qualifying file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the multiple multiline json files using the spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

qualifying_schema = StructType([
    StructField("qualifyId", IntegerType(), True),
    StructField("raceId", IntegerType(), True),
    StructField("driverId", IntegerType(), True),
    StructField("constructorId", IntegerType(), True),
    StructField("number", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("q1", StringType(), True),
    StructField("q2", StringType(), True),
    StructField("q3", StringType(), True)
])                      

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json("/mnt/f1datalake2025/raw/qualifying")


# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####add ingestion date and  rename
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                   .withColumnRenamed("constructorId", "constructor_id") \
                                   .withColumnRenamed("raceId", "race_id") \
                                   .withColumnRenamed("driverId", "driver_id") \
                                   .withColumn("ingestion_date", current_timestamp()) \
                                   .withColumn("file_date", lit(v_file_date))
display(qualifying_final_df)



# COMMAND ----------

# MAGIC %md
# MAGIC ######
# MAGIC write the file as parquet to cleaned and processed container

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
merge_delta_data(qualifying_final_df, 'f1_processed', 'qualifying', merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")