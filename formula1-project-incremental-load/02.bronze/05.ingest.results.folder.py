# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest results folder
# MAGIC - read the file using sprak dataframe reader API
# MAGIC - add metadat columns(1. spurce file , 2. ingestion timestamp)
# MAGIC - write the bronze delta table

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/02.bronze.helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/{v_batch_id}/results"
table_name = f"{catalog_name}.{bronze_schema}.results"

# COMMAND ----------

source_file

# COMMAND ----------

table_name

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 - read the file using sprak dataframe reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

results_schema = StructType(fields=[
    StructField("date", DateType()),
    StructField("raceName", StringType()),   # match JSON exactly
    StructField("round", IntegerType()),
    StructField("season", IntegerType()),
    StructField("url", StringType()),
    StructField("constructorId", StringType()),
    StructField("driverId", StringType()),
    StructField("grid", IntegerType()),
    StructField("laps", IntegerType()),
    StructField("number", IntegerType()),
    StructField("points", FloatType()),
    StructField("position", IntegerType()),
    StructField("positionText", StringType()),
    StructField("status", StringType())
])



# COMMAND ----------

results_df = (
    spark.read.format('json')
    .option('mode', 'FAILFAST')
    .schema(results_schema)
    .load(source_file)
)
display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 - Add metadat columns(1. spurce file , 2. ingestion timestamp)**

# COMMAND ----------

results_final_df = add_ingestion_metadata(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 - write the bronze delta table**

# COMMAND ----------

write_to_bronze (
    input_df=results_final_df, 
    target_table=table_name, 
    batch_id=v_batch_id
)


# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season, Count(*)
# MAGIC FROM formual1.bronze.results
# MAGIC GROUP BY season
# MAGIC ORDER BY season;