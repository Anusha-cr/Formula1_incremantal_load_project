# Databricks notebook source
# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.races"
silver_table = f"{catalog_name}.{silver_schema}.races"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **step-1 - read broze races table**

# COMMAND ----------

races_df = spark.table(bronze_table) # recommened in our project
display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step- 2 - keep only the columns required for analytics(drop url col)**

# COMMAND ----------

races_selected_df = races_df.select(
    F.col("season"),
    F.col("round"),
    F.col("raceName"),
    F.col("date"),
    F.col("circuitId"),
    F.col("ingestion_timestamp"),
    F.col("source_file")

)

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 3 & 4 standardise col name**
# MAGIC
# MAGIC 1. standrarize col name using snake_cases(circuitID -> circuitid, circuitName - > circuit_name)
# MAGIC 2. rename col to make more meaningful( lat -> lattitude)

# COMMAND ----------

races_renamed_df = (
    races_selected_df
        .withColumnsRenamed({"raceName": "race_name", 
                             "date": "race_date", 
                             "circuitId": "circuit_id"})
)


# COMMAND ----------

# MAGIC %md
# MAGIC **step-5 remove duplicate records**

# COMMAND ----------

races_distinct_df = races_renamed_df.dropDuplicates(["season", "round"])
display(races_distinct_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **step-6 - transform values of col race_name and circuit_id to title case**

# COMMAND ----------

races_final_df = (
    races_distinct_df
        .withColumn("race_name", F.initcap(F.col("race_name")))
    )
display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 7 write the transformed data to silver races table**

# COMMAND ----------


(
    races_final_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)
)
display(spark.table(silver_table))
