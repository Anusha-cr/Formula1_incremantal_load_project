# Databricks notebook source
dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/03.silver.helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.constructors"
silver_table = f"{catalog_name}.{silver_schema}.constructors"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **step-1 - read broze constructors table**

# COMMAND ----------

constructors_df = (
    spark.table(bronze_table)
    .filter(F.col("batch_id") == v_batch_id)
 ) # recommened in our project
display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step- 2 - keep only the columns required for analytics(drop url col)**

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

display(constructors_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 3 & 4 standardise col name**

# COMMAND ----------

constructors_renamed_df = (constructors_dropped_df
.withColumnsRenamed({"constructorID": "constructor_id",
                     "name": "constructor_name",
}))
display(constructors_renamed_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **step-5 remove duplicate records**

# COMMAND ----------

constructors_distinct_df = constructors_renamed_df.dropDuplicates(["constructor_id"])
display(constructors_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step-6 - transform values of col race_name and circuit_id to title case**

# COMMAND ----------

constructors_final_df = (
    constructors_distinct_df
    .withColumn("nationality", F.initcap(F.col("nationality")))
)
display(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 7 write the transformed data to silver races table**

# COMMAND ----------

# DBTITLE 1,Cell 17
# spark.sql(f"DROP TABLE IF EXISTS {silver_table}")
# (
#     constructors_final_df
#         .write
#         .mode("overwrite")
#         .format("delta")
#         .saveAsTable(silver_table)
# )

write_to_silver(
    input_df=constructors_final_df,
    target_table=silver_table,
    merge_condition="t.constructor_id = s.constructor_id",
    columns_to_update=[
        "constructor_name",
        "nationality",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)
display(spark.table(silver_table))