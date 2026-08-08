# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/02.bronze.helpers

# COMMAND ----------

#remove hard code
source_file = f"{landing_folder_path}/{v_batch_id}/circuits.csv"
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
    .option('mode', 'FAILFAST')
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

#def write_to_bronze (
    #input_df,
    #table_name,
    #batch_id
#):
    #final_df =
       # input_df
        #.withColumn('batch_id', F.lit(batch_id))
    #(
       # final_df
        #.write
        #.format('delta')
       # .mode('overwrite')
        #.option('overwriteSchema', 'true')
        #.partitionBy('batch_id')
      #  .option('replaceWhere', f"batch_id = '{batch_id}'")
      #  .saveAsTable(table_name)
    #)



# COMMAND ----------

#circuits_final_df = circuits_final_df.withColumn("batch_id", F.lit(v_batch_id))

# COMMAND ----------

# DBTITLE 1,Cell 16
#circuits_final_df = circuits_final_df.withColumn("batch_id", F.lit#(v_batch_id))
#(
    #circuits_final_df
   # .write
   # .format('delta')
   # .mode('overwrite')
    #.option('overwriteSchema', 'true')
    #.partitionBy('batch_id')
    #.option('replaceWhere', f"batch_id = '{v_batch_id}'")
    #.saveAsTable(table_name)
#)

# COMMAND ----------

write_to_bronze (
    input_df=circuits_final_df, 
    target_table=table_name, 
    batch_id=v_batch_id
    )

# COMMAND ----------

display(spark.table(table_name))