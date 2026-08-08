# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/03.silver.helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

sprints_df = (spark.table(bronze_table)
              .select("season", "round", "constructorId", "driverId", "date", "racename", "grid","laps", "number", "points", "position", "positionText", "status", "ingestion_timestamp", "source_file","batch_id")
    .withColumnsRenamed({
        "constructorId": "constructor_id",
        "driverId": "driver_id",
        "racename": "race_name",
        "date": "race_date",
        "grid": "grid_position",
        "laps": "completed_laps",
        "number": "car_number",
        "position": "final_position",
        "positionText": "final_position_text"
        })
       
        )

# COMMAND ----------

sprints_valid_df = (sprints_df.filter(
    F.col("season").isNotNull() & 
    F.col("round").isNotNull() & 
    F.col("constructor_id").isNotNull() & 
    F.col("driver_id").isNotNull() )
    .dropDuplicates(["season", "round", "constructor_id", "driver_id"])
)

# COMMAND ----------

display(sprints_df.count() - sprints_valid_df.count())

# COMMAND ----------

sprints_final_df = (sprints_valid_df.withColumn('race_name', F.initcap(F.col('race_name'))))

# COMMAND ----------

write_to_silver(
    input_df=sprints_final_df,
    target_table=silver_table,
    merge_condition="t.season = s.season AND t.round = s.round AND t.constructor_id = s.constructor_id AND t.driver_id = s.driver_id",
    columns_to_update=[
        "race_name",
        "race_date",
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ])
display(spark.table(silver_table))