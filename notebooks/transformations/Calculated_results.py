# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""CREATE TABLE IF NOT EXISTS f1_presentation.calculated_results
               (race_year INT,
               team_name STRING,
               driver_id INT,
               driver_name STRING,
               race_id INT,
               position INT,
               points INT,
               calculated_points INT,
               craeted_date TIMESTAMP,
               updated_date TIMESTAMP)
               USING DELTA""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW race_results_updated
AS
SELECT races.race_year, constructors.name AS team_name, drivers.driver_id, races.race_id, drivers.name AS driver_name, results.position, 11-results.position as calculated_points -- different points in different years
FROM f1_processed.results
JOIN f1_processed.drivers ON results.driver_id = drivers.driver_id
JOIN f1_processed.constructors ON results.constructor_id = constructors.constructor_id
JOIN f1_processed.races ON results.race_id = races.race_id
WHERE results.position<=10  --to get rid of -ve points
    AND results.file_date = '{v_file_date}'
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_presentation.calculated_results tgt
# MAGIC USING race_results_updated upd
# MAGIC ON (tgt.race_id = upd.race_id AND tgt.driver_id = upd.driver_id)
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET tgt.position = upd.position,
# MAGIC            tgt.calculated_points = upd.calculated_points,
# MAGIC            tgt.updated_date = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC THEN INSERT (race_year, team_name, driver_id, driver_name, race_id, position, calculated_points, craeted_date)
# MAGIC VALUES (race_year, team_name, driver_id, driver_name, race_id, position, calculated_points, current_timestamp())
# MAGIC