# Databricks notebook source
env = 'dev'

# COMMAND ----------

import os
import platform
def print_env_info():
    print("python version: {platform.python_version()}")
    runtime_version = os.environ.get("DATBRICKS_RUNTIME_VERSION", "Unknown")
    print(f"Databricks runtime version: {runtime_version}")


# COMMAND ----------

