-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Set up the environment for formual1 project
-- MAGIC 1.create external location databricks-course-ext-ld1-formula1-incr

-- COMMAND ----------

-- MAGIC %fs ls 'abfss://formula1-incr@databrickscouseextdl1.dfs.core.windows.net/landing'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create external location

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS databricks_course_ext_dl1_formula1_incr
URL 'abfss://formula1-incr@databrickscouseextdl1.dfs.core.windows.net/' 
WITH (STORAGE CREDENTIAL `databricks-course-sc`)
COMMENT 'External location for the formual1 container';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create catalog formula1

-- COMMAND ----------

SHOW CATALOGS;

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS `formula1_incr`
MANAGED LOCATION 'abfss://formula1-incr@databrickscouseextdl1.dfs.core.windows.net/'
COMMENT 'This is not the main catalog for the formula1 project' ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #create schemas landing,bronze,silver,gold

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS formula1_incr.landing;
CREATE SCHEMA IF NOT EXISTS formula1_incr.bronze
    MANAGED LOCATION 'abfss://formula1-incr@databrickscouseextdl1.dfs.core.windows.net/bronze';
CREATE SCHEMA IF NOT EXISTS formula1_incr.silver
    MANAGED LOCATION 'abfss://formula1-incr@databrickscouseextdl1.dfs.core.windows.net/silver';
CREATE SCHEMA IF NOT EXISTS formula1_incr.gold
    MANAGED LOCATION 'abfss://formula1-incr@databrickscouseextdl1.dfs.core.windows.net/gold';

-- COMMAND ----------

select current_catalog();

-- COMMAND ----------

use catalog formula1_incr;

-- COMMAND ----------

show schemas;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #creating volume files
-- MAGIC uses the external loaction to expose that storage as a managed folder within databricks

-- COMMAND ----------

-- DBTITLE 1,Cell 14
CREATE EXTERNAL VOLUME IF NOT EXISTS formula1_incr.landing.files
LOCATION 'abfss://formula1-incr@databrickscouseextdl1.dfs.core.windows.net/landing';

-- COMMAND ----------

-- MAGIC %fs ls /Volumes/formula1_incr/landing/files