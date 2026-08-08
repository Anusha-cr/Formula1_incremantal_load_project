# Databricks notebook source
# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.sprints"
silver_table = f"{catalog_name}.{silver_schema}.sprints"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

sprints_df = (spark.table(bronze_table)
              .select("season", "round", "constructorId", "driverId", "date", "racename", "grid","laps", "number", "points", "position", "positionText", "status", "ingestion_timestamp", "source_file")
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

(
    sprints_final_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)
)
display(spark.table(silver_table))