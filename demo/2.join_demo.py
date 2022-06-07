# Databricks notebook source
# MAGIC %run "../include/configuration"

# COMMAND ----------

circuits_df

# COMMAND ----------

races_df

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, how="inner")

# COMMAND ----------

#remember to assign unique column names BEFORE join tables (.withColumnRenamed())

# COMMAND ----------

#semi join: only records that match conditions of both tables, but get columns only from left data frame (like inner join and select columns after)

# COMMAND ----------

#anti join: opposite of semi join: everything on left df not available on right df

# COMMAND ----------

#cross join: every record from left, joined to every record on right, give product (carthesian product)
