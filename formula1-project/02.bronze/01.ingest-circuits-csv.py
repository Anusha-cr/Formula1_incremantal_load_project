# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest cirvuit.csv file
# MAGIC - read the file using sprak dataframe reader API
# MAGIC - add metadat columns(1. spurce file , 2. ingestion timestamp)
# MAGIC - write the bronze delta table

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/02.bronze.helpers

# COMMAND ----------

#remove hard code
source_file = f"{landing_folder_path}/circuits.csv"
table_name = f"{catalog_name}.{bronze_schema}.circuits"

# COMMAND ----------

source_file

# COMMAND ----------

table_name

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 1. read the file using sprak dataframe reader API**

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType
circuits_schema = StructType([
    StructField('circuitId', StringType()),
    StructField('url', StringType()),
    StructField('circuitName', StringType()),
    StructField('lat', DoubleType()),
    StructField('long', DoubleType()),
    StructField('locality', StringType()),
    StructField('country', StringType())])

# COMMAND ----------

circuits_df = (
    spark.read.format('csv')
    .option('header', 'true')
    #.option('inferSchema','true') - scans the table and provide the approriate datatype  for the columns. if we mentioned schema no need to mention inferSchema
    .option('mode', 'FAILFASTE')
    .schema(circuits_schema)
    .load(source_file)
)

# COMMAND ----------

circuits_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step - 2. Add metadata columns(1. source file , 2. ingestion timestamp)**

# COMMAND ----------


circuits_final_df = add_ingestion_metadata(circuits_df)

# COMMAND ----------

circuits_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 3.write the bronze delta table in the brnze schema**

# COMMAND ----------

(
    circuits_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM formual1.bronze.circuits;

# COMMAND ----------

# we can use this function to display the delta tableusing python
display(spark.table(table_name))

# COMMAND ----------

# MAGIC %md
# MAGIC Successfully injested circuit data from the landing layer to the bronze layer as a delta table

# COMMAND ----------

