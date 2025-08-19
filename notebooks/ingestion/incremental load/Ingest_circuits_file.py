# Databricks notebook source
# MAGIC %md
# MAGIC ### ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read the CSV file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_datasource", "")
v_data_source = dbutils.widgets.get("p_datasource")



# COMMAND ----------

print("current_user:", spark.sql("select current_user()").first()[0])


# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/config"
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                   StructField("circuitRef", StringType(), True),
                                   StructField("name", StringType(), True),
                                   StructField("location", StringType(), True),
                                   StructField("country", StringType(), True),
                                   StructField("lat", DoubleType(), True),
                                   StructField("lng", DoubleType(), True),
                                   StructField("alt", IntegerType(), True),
                                   StructField("url", StringType(), True)
                                   ])
                                  

# COMMAND ----------

circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")

#infer schema to apply schema from data to dataframe. this first reads the entire data, identify schema and then apply data hence 2 spark jobs
#so we specify the schema and use that to read the data, hence 1 job

# COMMAND ----------

# MAGIC %md
# MAGIC #####select the required column
# MAGIC

# COMMAND ----------

#circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))
circuits_selected_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####rename the columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude")  \
.withColumnRenamed("alt", "altitude") \
.withColumn("file_date", lit(v_file_date))
display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####add ingestion date to datafram
# MAGIC

# COMMAND ----------


circuits_final_df = add_ingestion_date(circuits_renamed_df)
display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ######
# MAGIC write the file as parquet to cleaned and processed container

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")