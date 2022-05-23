# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Ingest constructors.json file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json(f'{raw_folder_path}/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted column from data frame

# COMMAND ----------

drop_constructor_df = constructor_df.drop(constructor_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - rename columns & Add ingestion date

# COMMAND ----------

almost_final_constructor_df = drop_constructor_df.withColumnRenamed('constructorId','constructor_id') \
                                          .withColumnRenamed('constructorRef','constructor_ref')

# COMMAND ----------

final_constructor_df = add_ingestion_date(almost_final_constructor_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to file

# COMMAND ----------

final_constructor_df.write.mode('overwrite').parquet(f'{processed_folder_path}/constructors')
