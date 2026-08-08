-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Configure access to cloud storage via unity catalog

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## # Access cloud storage

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://demo@databrickscouseextdl1.dfs.core.windows
-- MAGIC
-- MAGIC .net/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## # Create External Location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_demo
URL 'abfss://demo@databrickscouseextdl1.dfs.core.windows.net/' 
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the demo container';

-- COMMAND ----------

