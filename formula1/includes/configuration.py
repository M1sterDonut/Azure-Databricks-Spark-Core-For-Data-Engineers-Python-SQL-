# Databricks notebook source
# MAGIC %md
# MAGIC **Use** a **config file (or any other separate notebook)** to define code that:
# MAGIC 1. may change (folder paths)
# MAGIC 2. is reused across notebooks (e.g define functions)

# COMMAND ----------

raw_folder_path = '/mnt/formula123dl/raw'
processed_folder_path = '/mnt/formula123dl/processed'
