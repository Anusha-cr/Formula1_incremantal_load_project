# Databricks notebook source
# MAGIC %md
# MAGIC ### Build Nationality Region Referenece
# MAGIC - 1. create a dataframe with list of nationalities and the corresponding geographic regions
# MAGIC - 2. write the dataframe to gold ref_nationality_region_table

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.ref_nationality_region"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **reate a dataframe with list of nationalities and the corresponding geographic regions**

# COMMAND ----------

from pyspark.sql import Row

nationality_region_map_rows= [
    Row(nationality="British", region="Europe"),
    Row(nationality="Italian", region="Europe"),
    Row(nationality="French", region="Europe"),
    Row(nationality="German", region="Europe"),
    Row(nationality="Swiss", region="Europe"),
    Row(nationality="Dutch", region="Europe"),
    Row(nationality="Belgian", region="Europe"),
    Row(nationality="Irish", region="Europe"),
    Row(nationality="Spanish", region="Europe"),
    Row(nationality="Austrian", region="Europe"),
    Row(nationality="East German", region="Europe"),
    Row(nationality="Russian", region="Europe"),
    Row(nationality="Finnish", region="Europe"),
    Row(nationality="Polish", region="Europe"),
    Row(nationality="Portuguese", region="Europe"),
    Row(nationality="Hungarian", region="Europe"),
    Row(nationality="Danish", region="Europe"),
    Row(nationality="Czech", region="Europe"),
    Row(nationality="Liechtensteiner", region="Europe"),
    Row(nationality="Monegasque", region="Europe"),
    Row(nationality="Swedish", region="Europe"),

    #nort american
    Row(nationality="American", region="North America"),
    Row(nationality="Canadian", region="North America"),
    Row(nationality="Mexican", region="North America"),
    Row(nationality="Cuban", region="North America"),

    #south american
    Row(nationality="Argentine", region="South America"),
    Row(nationality="Brazilian", region="South America"),
    Row(nationality="Chilean", region="South America"),
    Row(nationality="Colombian", region="South America"),
    Row(nationality="Uruguayan", region="South America"),
    Row(nationality="Paraguayan", region="South America"),
    Row(nationality="Peruvian", region="South America"),
    Row(nationality="Venezuelan", region="South America"),

    #African
    Row(nationality="South African", region="Africa"),
    Row(nationality="Rhodesian", region="Africa"),

    #asian
    Row(nationality="Chinese", region="Asia"),
    Row(nationality="Japanese", region="Asia"),
    Row(nationality="Indian", region="Asia"),
    Row(nationality="Korean", region="Asia"),
    Row(nationality="Malaysian", region="Asia"),
    Row(nationality="Singaporean", region="Asia"),
    Row(nationality="Thai", region="Asia"),
    Row(nationality="Hong Kong", region="Asia"),

    #oceasian
    Row(nationality="Australian", region="Oceania"),
    Row(nationality="New Zealander", region="Oceania"),
    Row(nationality="Fijian", region="Oceania"),
    Row(nationality="Papua New Guinean", region="Oceania"),
    Row(nationality="New Zealand", region="Oceania")
 
]
ref_nationality_region_df = spark.createDataFrame(nationality_region_map_rows)


# COMMAND ----------

(
    ref_nationality_region_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

display(spark.table(target_table))