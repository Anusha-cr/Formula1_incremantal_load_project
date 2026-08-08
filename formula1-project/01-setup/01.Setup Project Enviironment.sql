-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Set up the environment for formual1 project
-- MAGIC 1.create external location databricks-course-ext-ld1-formula1

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formual1@databrickscouseextdl1.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create external location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_formula1
URL 'abfss://formual1@databrickscouseextdl1.dfs.core.windows.net/' 
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the formual1 container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create catalog formula1

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS formual1
MANAGED LOCATION 'abfss://formual1@databrickscouseextdl1.dfs.core.windows.net/'
COMMENT 'This is not the main catalog for the formula project' ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create schemas landing,bronze,silver,gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formual1.landing;
CREATE SCHEMA IF NOT EXISTS formual1.bronze
    MANAGED LOCATION 'abfss://formual1@databrickscouseextdl1.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formual1.silver
    MANAGED LOCATION 'abfss://formual1@databrickscouseextdl1.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formual1.gold
    MANAGED LOCATION 'abfss://formual1@databrickscouseextdl1.dfs.core.windows.net/gold';

-- COMMAND ----------

select current_catalog();

-- COMMAND ----------

use catalog formual1;

-- COMMAND ----------

show schemas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #creating volume files
-- MAGIC uses the external loaction to expose that storage as a managed folder within databricks

-- COMMAND ----------

CREATE EXTERNAL VOLUME formual1.landing.files
LOCATION 'abfss://formual1@databrickscouseextdl1.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formual1/landing/files