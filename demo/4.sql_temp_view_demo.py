# Databricks notebook source
### Access dataframes using SQL

#Objectives: 1) create temp view on dataframes, 2) access view from SQL cell, 3) access view from Python cell

# COMMAND ----------

# MAGIC %run "../formula1/includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# 1) temp view on dataframe

# df.createTempView("df_name")

races_df.createOrReplaceTempView("v_races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM v_races
# MAGIC WHERE race_year = 2020

# COMMAND ----------

display(spark.sql("SELECT * FROM v_races WHERE race_year = 2019"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_temp.gv_race_results
