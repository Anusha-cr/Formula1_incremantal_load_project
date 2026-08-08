# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ###Build constructor  dimension
# MAGIC - 1.read silver constructor table
# MAGIC - 2.read gold ref_nationality_region table
# MAGIC - 3.join data from constructor with ref_nationality_region using nationality
# MAGIC - 4.select the required columns
# MAGIC -     constructors_constructor_id
# MAGIC -     constructors_constructor_name
# MAGIC -     constructors_nationality
# MAGIC -     ref_nationalty_region.region
# MAGIC - 5.write the transmed data to dim_constructors table

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_constructors"

# COMMAND ----------

# MAGIC %md
# MAGIC step 1 read silver constructor table
# MAGIC - silver constructors
# MAGIC - gold.ref_nationality_region

# COMMAND ----------

constructors_df = spark.table(f"{catalog_name}.{silver_schema}.constructors")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ### step 2 join data from constructor with ref_nationality_region using nationality
# MAGIC .select the required columns
# MAGIC - constructors_constructor_id
# MAGIC - constructors_constructor_name
# MAGIC - constructors_nationality
# MAGIC - ref_nationalty_region.region

# COMMAND ----------

dim_constructors_df = (constructors_df
                       .join(ref_nationality_region_df,
                        constructors_df.nationality == ref_nationality_region_df.nationality,
                       "left")
                       .select(constructors_df.constructor_id,
                               constructors_df.constructor_name,
                               constructors_df.nationality,
                               ref_nationality_region_df.region.alias("nationality_region")))

# COMMAND ----------

display(dim_constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **write the transmed data to dim_constructors table**

# COMMAND ----------

(
    dim_constructors_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)
)

# COMMAND ----------

display(spark.table(target_table))