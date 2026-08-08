# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest races.csv file
# MAGIC - read the file using sprak dataframe reader API
# MAGIC - add metadat columns(1. spurce file , 2. ingestion timestamp)
# MAGIC - write the bronze delta table

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/02.bronze.helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/races.csv"
table_name = f"{catalog_name}.{bronze_schema}.races"

# COMMAND ----------

table_name

# COMMAND ----------

# MAGIC %md
# MAGIC **step 1 - read the file using sprak dataframe reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
races_schema = StructType([
    StructField('season', IntegerType()),
    StructField('round', IntegerType()),
    StructField('url', StringType()),
    StructField('raceName', StringType()),
    StructField('date', DateType()),
    StructField('circuitId', StringType())
])

# COMMAND ----------

races_df = (
    spark.read.format('csv')
    .option('header', 'true')
    #.option('inferSchema','true') - scans the table and provide the approriate datatype  for the columns. if we mentioned schema no need to mention inferSchema
    .option('mode', 'FAILFAST')
    .schema(races_schema)
    #.option('inferSchema','true')
    .load(source_file)
)

# COMMAND ----------

races_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step - 2 add metadat columns(1. spurce file , 2. ingestion timestamp)**

# COMMAND ----------


races_final_df = add_ingestion_metadata(races_df)


# COMMAND ----------

races_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 3. write the bronze delta table**

# COMMAND ----------

(
    races_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)


# COMMAND ----------

display(spark.table(table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC Successfully injested races data from the landing layer to the bronze layer as a delta table

# COMMAND ----------

