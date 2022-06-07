# Databricks notebook source
v_results = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": "Ergast API"})

# COMMAND ----------

### do the same for all other files

# COMMAND ----------

### Databricks documentation: run multiple notebooks concurrently

# COMMAND ----------

### wouldn't use DB in production environment, rather smth like Azure DataFactory
