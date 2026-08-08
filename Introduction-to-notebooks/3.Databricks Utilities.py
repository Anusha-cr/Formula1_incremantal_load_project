# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Utilites
# MAGIC - File system utilites
# MAGIC - Secret utilities
# MAGIC - Widget Utilites
# MAGIC - Notebook workflow utilities

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. File system utiliies

# COMMAND ----------

# MAGIC %md
# MAGIC ls - list all the files in the current directory 

# COMMAND ----------

# MAGIC %fs ls /

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.fs.ls - list all the files and folder in the specified directory and retuns a python list

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

# MAGIC %md
# MAGIC display command - only available in pyhton,scala and r. cannot use in SQL

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

# MAGIC %md
# MAGIC help-to veiw all the available utilities in the databricks

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.fs.help() - to veiw all the file syetem utilities in databricks

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help('cp')