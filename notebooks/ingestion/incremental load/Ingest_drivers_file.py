# Databricks notebook source
# MAGIC %md
# MAGIC ####ingest drivers file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields = [StructField("forename", StringType(), True),
                                  StructField("surname", StringType(), True)])

# COMMAND ----------

driver_schema = StructType(fields = (StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("code", StringType(), True),
                                     StructField("name", name_schema),
                                     StructField("dob", DateType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True)))

# COMMAND ----------

driver_df = spark.read \
.schema(driver_schema) \
.json(f"/mnt/f1datalake2025/raw/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #####rename the columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, concat, lit

# COMMAND ----------

driver_renamed_df = driver_df \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("driverRef", "driver_ref") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("name", concat("name.forename", lit(" "), "name.surname")) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(driver_renamed_df)

# COMMAND ----------

driver_final_df = driver_renamed_df.drop("url")

# COMMAND ----------

driver_final_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")