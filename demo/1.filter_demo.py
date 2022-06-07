# Databricks notebook source
 ### choose folder path

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

#SQL way -- can use .filter / .where interchangably!
races_filtered_df = races_df.where("race_year = 2019 AND round <=5")

# COMMAND ----------

#Python way
races_filtered_df = races_df.filter(races_df["race_year"] == 2019 & races_df["round"] <= 5)
