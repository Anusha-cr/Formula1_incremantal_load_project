# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest constructive.json file
# MAGIC - read the file using sprak dataframe reader API
# MAGIC - add metadat columns(1. spurce file , 2. ingestion timestamp)
# MAGIC - write the bronze delta table

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/02.bronze.helpers

# COMMAND ----------

source_file = f"{landing_folder_path}/constructors.json"
table_name = f"{catalog_name}.{bronze_schema}.constructors"

# COMMAND ----------

source_file

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 1 read the file using sprak dataframe reader API**

# COMMAND ----------

constructor_schema = "constructorId string, name string, nationality string, url string"

# COMMAND ----------

constructor_df = (
    spark.read.format('json')
    #.option('header', 'true')
    #.option('inferSchema','true') - scans the table and provide the approriate datatype  for the columns. if we mentioned schema no need to mention inferSchema
    .option('mode', 'FAILFASTE')
    .schema(constructor_schema)
    .load(source_file)

)

# COMMAND ----------

constructor_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 2.Add metadata columns(1. source file , 2. ingestion timestamp)**

# COMMAND ----------

constructor_final_df = add_ingestion_metadata(constructor_df)

# COMMAND ----------

constructor_final_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Step - 3.write the bronze delta table in the brnze schema**

# COMMAND ----------

(
    constructor_final_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(table_name)
)

# COMMAND ----------

display(spark.table(table_name))