# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Mgaic Commands
# MAGIC - %python,%Scala,%SQL,%r : Switch to differnt language for a specific cell
# MAGIC - %md : markdpown for socumenting notebooks
# MAGIC - %fs : Run file system command
# MAGIC - %sh : Run shell command (Driver node only)
# MAGIC - %pip : instal python libraries
# MAGIC - %run : include/import another notebooks into the current notebook

# COMMAND ----------

# MAGIC %md
# MAGIC %fs : Run file system command
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /
# MAGIC

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets/

# COMMAND ----------

# MAGIC %md
# MAGIC %sh : Run shell command (Driver node only) - we can see all the process running in driver node

# COMMAND ----------

# MAGIC %sh
# MAGIC ps

# COMMAND ----------

# MAGIC %md
# MAGIC %pip : instal python libraries

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

# MAGIC %pip install faker

# COMMAND ----------

# MAGIC %md
# MAGIC %run : include/import another notebooks into the current notebook

# COMMAND ----------

# MAGIC %run "/Workspace/Users/arvindbiradar2863@gmail.com/Databricks-Course/Introduction-to-notebooks/2.1 Environment varibale"

# COMMAND ----------

env 

# COMMAND ----------

print_env_info()

# COMMAND ----------

# MAGIC %md
# MAGIC we imported one notebook(env varibale) to amother notebbok(magic command)