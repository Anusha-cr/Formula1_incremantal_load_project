# Databricks notebook source
# MAGIC %md
# MAGIC **Build Results fact**
# MAGIC - - read silevr results table
# MAGIC - read silver sprints table
# MAGIC - add new column session_type with values race or sprint
# MAGIC - union results and sprints
# MAGIC - driver additional columns
# MAGIC - 1.is_win = indicates that teh driver own teh race
# MAGIC - 2.is_podium = indicates that the driver scored a podium results (1, 2,3)
# MAGIC - 3.has_points = indicates the driver has scored points
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/04.gold.helper

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.fact_session_results"

# COMMAND ----------

# MAGIC %md
# MAGIC step 1 - read source tables
# MAGIC - 1.silver results
# MAGIC - 2.silver.sprints

# COMMAND ----------

results_df = (
    spark.table(f"{catalog_name}.{silver_schema}.results")
    .filter(F.col("batch_id") == v_batch_id)  # Ensure v_batch_id is defined before this line
    .withColumn("session_type", F.lit("RACE"))
    .drop("race_name", "race_date", "circuit_id", "ingestion_timestamp", "source_file")
)


# COMMAND ----------

display(results_df)

# COMMAND ----------

sprints_df = (spark.table(f"{catalog_name}.{silver_schema}.results")
.filter(F.col("batch_id") == v_batch_id)            
.withColumn("session_type", F.lit("SPRINT"))
.drop("race_name", "race_date", "circuit_id", "ingestion_timestamp", "source_file")
)
display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Step-2 union results and sprints

# COMMAND ----------

results_sprints_df = results_df.unionByName(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC step - 3 
# MAGIC - 1.is_win = indicates that teh driver own teh race
# MAGIC - 2.is_podium = indicates that the driver scored a podium results (1, 2,3)
# MAGIC - 3.has_points = indicates the driver has scored points

# COMMAND ----------

fact_session_results_df = (
    results_sprints_df
    .withColumn("is_win", F.col("final_position") == 1)
    .withColumn("is_podium", F.col("final_position").between(1, 3))
    .withColumn("has_points", F.col("points") > 0)
)

# COMMAND ----------

display(fact_session_results_df.filter("season = 2025"))

# COMMAND ----------

# MAGIC %md
# MAGIC step-4

# COMMAND ----------

write_to_gold(
    input_df=fact_session_results_df,
    target_table=target_table,
    merge_condition="""
    t.season = s.season
    AND t.round = s.round
    AND t.constructor_id = s.constructor_id
    AND t.driver_id = s.driver_id
    AND t.session_type = s.session_type""",
    columns_to_update=[
        "grid_position",
        "completed_laps",
        "car_number",
        "points",
        "final_position",
        "final_position_text",
        "status",
        "is_win",
        "is_podium",
        "has_points"
    ]
)
display(spark.table(target_table))