# Databricks notebook source
# MAGIC %md
# MAGIC ###Build constructor dimension
# MAGIC - 1.read silver driver table
# MAGIC - 2.read gold ref_nationality_region table
# MAGIC - 3.join data from driver with ref_nationality_region using nationality
# MAGIC - 4.select the required columns
# MAGIC - drivers_driver_id
# MAGIC - drivrers_driver_name
# MAGIC - drivers_dob
# MAGIC - drivers_nationality
# MAGIC - ref_nationalty_region.region
# MAGIC - 5.write the transmed data to dim_constructors table

# COMMAND ----------

# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

target_table = f"{catalog_name}.{gold_schema}.dim_drivers"

# COMMAND ----------

# MAGIC %md
# MAGIC step 1 read silver constructor table
# MAGIC
# MAGIC - silver constructors
# MAGIC - gold.ref_nationality_region

# COMMAND ----------

drivers_df = spark.table(f"{catalog_name}.{silver_schema}.drivers")
ref_nationality_region_df = spark.table(f"{catalog_name}.{gold_schema}.ref_nationality_region")

# COMMAND ----------

# MAGIC %md
# MAGIC ###
# MAGIC  step 2 join data from constructor with ref_nationality_region using nationality
# MAGIC .select the required columns
# MAGIC
# MAGIC - drivers_driver_id
# MAGIC - drivrers_driver_name
# MAGIC - drivers_dob
# MAGIC - drivers_nationality
# MAGIC - ref_nationalty_region.region

# COMMAND ----------

dim_drivers_df = (drivers_df
                       .join(ref_nationality_region_df,
                        drivers_df.nationality == ref_nationality_region_df.nationality,
                       "left")
                       .select(drivers_df.driver_id,
                               drivers_df.driver_name,
                               drivers_df.date_of_birth,
                               drivers_df.nationality,
                               ref_nationality_region_df.region.alias("nationality_region")))

# COMMAND ----------

display(dim_drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC step 3

# COMMAND ----------

(
    dim_drivers_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(target_table)

)

# COMMAND ----------

display(spark.table(target_table))