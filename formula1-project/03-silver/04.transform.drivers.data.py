# Databricks notebook source
# MAGIC %run ../00-common-confirguration/01.environment.config

# COMMAND ----------

bronze_table = f"{catalog_name}.{bronze_schema}.drivers"
silver_table = f"{catalog_name}.{silver_schema}.drivers"

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC **step-1 - read broze drivers table**

# COMMAND ----------

drivers_df = spark.table(bronze_table) # recommened in our project
display(drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step- 2 - keep only the columns required for analytics(drop url col)**

# COMMAND ----------

drivers_dropped_df = drivers_df.drop('url')
#display(drivers_dropped_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **step - 3 & 4 standardise col name**

# COMMAND ----------

drivers_renamed_df = (
    drivers_dropped_df
        .withColumnsRenamed({"driverId": "driver_id", 
                             "dateOfBirth": "date_of_birth"})
)
display(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step - 4 - concatenate name.givenName and name.familyName to create new col called driver_name**

# COMMAND ----------

drivers_concatenated_df = (
    drivers_renamed_df
        .withColumn("driver_name", F.initcap(F.concat_ws(' ', F.col("name.givenName"), F.col("name.familyName"))))
        .drop("name")
)
display(drivers_concatenated_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **step-5 remove duplicate records**

# COMMAND ----------

drivers_distinct_df = drivers_concatenated_df.dropDuplicates(["driver_id"])
display(drivers_distinct_df)

# COMMAND ----------

# MAGIC %md
# MAGIC **step-6 - transform values of col nationality to title case**

# COMMAND ----------

drivers_final_df = (
    drivers_distinct_df
        .withColumn("nationality", F.initcap(F.col("nationality")))
)
display(drivers_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC **step - 6 write the transformed data to silver races table**

# COMMAND ----------

(
    drivers_final_df
        .write
        .mode("overwrite")
        .format("delta")
        .saveAsTable(silver_table)
)
display(spark.table(silver_table))