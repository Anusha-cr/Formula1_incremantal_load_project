-- Databricks notebook source
-- DBTITLE 1,Create control schema
CREATE SCHEMA IF NOT EXISTS formual1.control
MANAGED LOCATION 'abfss://formual1@databrickscouseextdl1.dfs.core.windows.net/control'

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS formual1.control.batch_events
(
    batch_id INT,
    event_timestamp TIMESTAMP
)

-- COMMAND ----------

-- DBTITLE 1,Insert batch event
INSERT INTO formual1.control.batch_events
VALUES (1, current_timestamp());

-- COMMAND ----------

INSERT INTO formual1.control.batch_events
VALUES (2, current_timestamp());

-- COMMAND ----------

SELECT * FROM formual1.control.batch_events;