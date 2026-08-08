# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest sprints folder
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

source_file = f"{landing_folder_path}/{v_batch_id}/sprints"
table_name = f"{catalog_name}.{bronze_schema}.sprints"

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 1 - read the file using sprak dataframe reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType

sprints_schema = StructType(fields=[
    StructField("date", StringType()),
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

sprints_df = (
    spark.read
    .format('json')
    .schema(sprints_schema)
    .option('mode', 'FAILFAST')
    .option('multiLine', 'true')
    .load(source_file)
)

# COMMAND ----------

display(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 - Add metadat columns(1. spurce file , 2. ingestion timestamp)**

# COMMAND ----------

sprint_final_df = add_ingestion_metadata(sprints_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 - write the bronze delta table**

# COMMAND ----------

write_to_bronze (
    input_df=sprint_final_df, 
    target_table=table_name, 
    batch_id=v_batch_id
)

# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT season, COUNT(*)
# MAGIC FROM formual1.bronze.sprints
# MAGIC GROUP BY season
# MAGIC ORDER BY season;