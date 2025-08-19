# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the CSV file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

races_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", StringType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])
                                  

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"/mnt/f1datalake2025/raw/{v_file_date}/races.csv")


# COMMAND ----------

display(races_df)

# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

races_df.describe().show()


# COMMAND ----------

# MAGIC %md
# MAGIC #####add ingestion date and  race timestamp
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit, col

# COMMAND ----------

races_wdatetime_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
                             .withColumn("ingestion_date", current_timestamp()) \
                             .withColumn("data_source", lit(v_data_source)) \
                             .withColumn("file_date", lit(v_file_date))
display(races_wdatetime_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #####select the required column and rename
# MAGIC

# COMMAND ----------


from pyspark.sql.functions import col
races_selected_df = races_wdatetime_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), 
                                              col("circuitId").alias("circuit_id"), col("name"), col("race_timestamp"), col("ingestion_date"), col("data_source"), col("file_date"))
display(races_selected_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ######
# MAGIC write the file as parquet to cleaned and processed container

# COMMAND ----------

races_selected_df.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("race_year") \
    .option("mergeSchema", "true") \
    .saveAsTable("f1_processed.races")


# COMMAND ----------

dbutils.notebook.exit("Success")