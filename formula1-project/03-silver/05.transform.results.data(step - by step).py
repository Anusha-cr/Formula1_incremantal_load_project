# Databricks notebook source
# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.results"
silver_table = f"{catalog_name}.{silver_schema}.results"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

results_df = spark.table(bronze_table) # recommened in our project
#display(results_df)

# COMMAND ----------


results_selected_df = (results_df
                       .select("season", "round", "constructorId", "driverId", "date", "racename", "grid","laps", "number", "points", "position", "positionText", "status", "ingestion_timestamp", "source_file")
                      )


# COMMAND ----------

results_renamed_df = (results_selected_df
                       .withColumnRenamed("constructorId", "constructor_id")
                       .withColumnRenamed("driverId", "driver_id")
                       .withColumnRenamed("racename", "race_name")
                       .withColumnRenamed("date", "race_date")
                       .withColumnRenamed("grid", "grid_position")
                       .withColumnRenamed("laps", "completed_laps")
                       .withColumnRenamed("number", "car_number")
                       .withColumnRenamed("position", "final_position")
                       .withColumnRenamed("positionText", "final_position_text")
                      )
#display(results_renamed_df)

# COMMAND ----------


results_valid_df = results_renamed_df.filter(
    F.col("season").isNotNull() & 
    F.col("round").isNotNull() & 
    F.col("constructor_id").isNotNull() & 
    F.col("driver_id").isNotNull() 
)

# COMMAND ----------

display(results_renamed_df.count() - results_valid_df.count())

# COMMAND ----------

results_distinct_df = results_valid_df.dropDuplicates(["season", "round", "constructor_id", "driver_id"])

# COMMAND ----------

display(results_valid_df.count() - results_distinct_df.count())

# COMMAND ----------

results_final_df = (results_distinct_df.withColumn('race_name', F.initcap(F.col('race_name'))))

# COMMAND ----------

(
    results_final_df
      .write
      .mode("overwrite")
      .format("delta")
      .saveAsTable(silver_table)
)

# COMMAND ----------

display(spark.table(silver_table))