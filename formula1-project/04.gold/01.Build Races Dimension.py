# Databricks notebook source
# MAGIC %md
# MAGIC ### Build races dimension
# MAGIC 1. read silver rces table
# MAGIC 2. read silver circuits table
# MAGIC 3. jin data from races with circuits using circuit_id
# MAGIC 4. select the required columns
# MAGIC     * races seasons
# MAGIC     * races_round
# MAGIC     * races'-race_name
# MAGIC     * races_race_date
# MAGIC     * circuits_circuit_name
# MAGIC     * circuits locality
# MAGIC     * circuits_country
# MAGIC 5. write the transmed data to gold_races table

# COMMAND ----------

# MAGIC %md
# MAGIC entity daisgram 1
# MAGIC entity disgarma for gold 2

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_races"

# COMMAND ----------

# MAGIC %md
# MAGIC step 1 - Read Source table
# MAGIC  1. circuits
# MAGIC  2. races

# COMMAND ----------

circuits_df = spark.table(f"{catalog_name}.{silver_schema}.circuits")
races_df = spark.table(f"{catalog_name}.{silver_schema}.races")


# COMMAND ----------

# MAGIC %md
# MAGIC step 2 - join data from races with circuits using circuit_id

# COMMAND ----------

# MAGIC %md
# MAGIC select the required columns
# MAGIC - races seasons
# MAGIC - races_round
# MAGIC - races'-race_name
# MAGIC - races_race_date
# MAGIC - circuits_circuit_name
# MAGIC - circuits locality
# MAGIC - circuits_country

# COMMAND ----------

dim_races_df = (
    races_df 
    .join(
        circuits_df, 
        races_df.circuit_id == circuits_df.circuit_id,
        "inner"
    )
    .select(
        races_df.season,
        races_df.round,
        races_df.race_name,
        races_df.race_date,
        circuits_df.circuit_name,
        circuits_df.locality,
        circuits_df.country
    )
)

# COMMAND ----------

display(dim_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC step 3 - write the transmed data to gold_races table

# COMMAND ----------

(
    dim_races_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))