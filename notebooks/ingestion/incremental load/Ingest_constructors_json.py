# Databricks notebook source
# MAGIC %md
# MAGIC ####Ingest_constructors_json

# COMMAND ----------

# MAGIC %md
# MAGIC ######Step 1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/config"

# COMMAND ----------


constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructor_df = spark.read \
.schema(constructor_schema) \
.json(f"/mnt/f1datalake2025/raw/{v_file_date}/constructors.json")



# COMMAND ----------

display(constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ######drop the url column

# COMMAND ----------

constructor_dropped_df = constructor_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ######rename columns and add time column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp()) \
                                             .withColumn("data_source", lit(v_data_source)) \
                                             .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

display(constructor_final_df)


# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")