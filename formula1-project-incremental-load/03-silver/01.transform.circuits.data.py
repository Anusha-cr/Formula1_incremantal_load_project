# Databricks notebook source
# MAGIC %md
# MAGIC #Transform Circuits data
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Below changes are required to implement load processing
# MAGIC
# MAGIC 1. accept batch_id as a parameter to the notebook
# MAGIC 2. process data for ony the batch_id being passed in(i.e, filter recording from bronze using batch_id)
# MAGIC 3. add created_timestamp, updated_timstamp and batch_id to the silver table
# MAGIC 4. merge the processed data to the silver table
# MAGIC - created_timestamp should only be populated at the timstamp inserting/ creating the record.it shpuld not be updated during the merge update
# MAGIC - ensure that we are not overwritting the data in silver table to older bronze dat(re-run scenario)

# COMMAND ----------

dbutils.widgets.text("p_batch_id", "")
v_batch_id = dbutils.widgets.get("p_batch_id")

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config
# MAGIC

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/03.silver.helper

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.circuits"
silver_table = f"{catalog_name}.{silver_schema}.circuits"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **step-1 - read broze circuits table**

# COMMAND ----------

circuits_df = (
    spark.table(bronze_table)
    .filter(F.col("batch_id") == v_batch_id)
    )
display(circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step- 2 - keep only the columns required for analytics(drop url col)**

# COMMAND ----------

circuits_selected_df = circuits_df.select(
    F.col("circuitId"),                            #using col expersion gived more flexibility
    F.col("circuitName"),
    F.col("lat"),
    F.col("long"),
    F.col("locality"),
    F.col("country"), 
    F.col("ingestion_timestamp"),
    F.col("source_file"),
    F.col("batch_id")
)

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 3 & 4 standardise col name**
# MAGIC 1. standrarize col name using snake_cases(circuitID -> circuitid, circuitName - > circuit_name)
# MAGIC 2. rename col to make more meaningful( lat -> lattitude)

# COMMAND ----------


circuits_renamed_df = (
    circuits_selected_df
        .withColumnsRenamed({"circuitId": "circuit_id", "circuitName": "circuit_name", "lat": "latitude", "long": "longitude"})
    )


# COMMAND ----------

# MAGIC %md
# MAGIC **step-5 Filter out rows where circuit_id is numm (business key validation)**

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

#remove numm values
circuits_valid_df = (
    circuits_renamed_df
        .filter("circuit_id IS NOT NULL")
)


# COMMAND ----------

display(circuits_valid_df)

# COMMAND ----------

#co function another way to remove null values from a column
circuits_valid_df = circuits_renamed_df.filter(
    F.col("circuit_id").isNotNull()
    )


# COMMAND ----------

# MAGIC %md
# MAGIC **step-6 remove duplicate records**

# COMMAND ----------

# remove duplicated 1 st way is distict method(apply on entire record)
circuits_distinct_df = circuits_valid_df.distinct()
display(circuits_distinct_df)

# remove duplicated 2 nd way is dropDuplicates method(apply on based on col name)
circuits_distinct_df = circuits_valid_df.dropDuplicates(["circuit_id"])
display(circuits_distinct_df)



# COMMAND ----------

# MAGIC %md
# MAGIC **step-7 - transform values of col circuit_name and locaality to title case**

# COMMAND ----------

#initcap function to convert the sentence into title case (Anusha anu -> Anusha Anu)
circuits_final_df = (
    circuits_distinct_df
        .withColumn("circuit_name", F.initcap(F.col("circuit_name")))
        .withColumn("locality", F.initcap(F.col("locality")))
    )
display(circuits_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **step - 8 write the transformed data to silver circuits table**

# COMMAND ----------

    # circuits_final_df = (
    #     circuits_final_df
    #     .withColumn("created_timestamp", F.current_timestamp())
    #     .withColumn("updated_at",F.current_timestamp())
    # )

# COMMAND ----------

# DBTITLE 1,Cell 24
write_to_silver(
    input_df=circuits_final_df,
    target_table=silver_table,
    merge_condition="t.circuit_id = s.circuit_id",
    columns_to_update=[
        "circuit_name",
        "latitude",
        "longitude",
        "locality",
        "country",
        "ingestion_timestamp",
        "source_file",
        "batch_id"
    ]
)


# COMMAND ----------

# write_to_silver (
#     input_df = circuits_final_df,
#     target_table = silver_table,
#     merge_condition = "t.circuit_id = s.circuit_id",
#     columns_to_update = [
#         "circuit_name",
#         "latitude",
#         "longitude",
#         "locality",
#         "country",
#         "ingestion_timestamp",
#         "source_file",
#         "batch_id"
#     ]
# )

# COMMAND ----------

# DBTITLE 1,Cell 25
display(spark.table(silver_table))