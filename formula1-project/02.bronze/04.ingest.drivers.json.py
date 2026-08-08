# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest results folder
# MAGIC - read the file using sprak dataframe reader API
# MAGIC - add metadat columns(1. spurce file , 2. ingestion timestamp)
# MAGIC - write the bronze delta table

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/02.bronze.helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/drivers.json"
table_name = f"{catalog_name}.{bronze_schema}.drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 1 read the file using sprak dataframe reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, TimestampType,   DateType

# inner schema
name_schema = StructType([
    StructField("givenName", StringType()),
    StructField("familyName", StringType())
])

# driver schema
drivers_schema = StructType([
    StructField("driverId", StringType()),
    StructField("name", name_schema),
    StructField("dateOfBirth", DateType()),  # match JSON field name exactly
    StructField("nationality", StringType()),
    StructField("url", StringType())
])


# COMMAND ----------

drivers_df = (
    spark.read.format("json")
    .schema(drivers_schema)
    .option("mode", "FAILFAST")
    .load(source_file)
)
display(drivers_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **Step 2 - Add metadat columns(1. spurce file , 2. ingestion timestamp)**

# COMMAND ----------

drivers_final_df = add_ingestion_metadata(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **Step 3 - write the bronze delta table**

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {table_name}")

(
    drivers_final_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(table_name)
)


# COMMAND ----------

display(spark.table(table_name))