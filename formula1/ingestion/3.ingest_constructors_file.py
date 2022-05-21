# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Ingest constructors.json file

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read \
    .schema(constructors_schema) \
    .json('/mnt/formula123dl/raw/constructors.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted column from data frame

# COMMAND ----------

drop_constructor_df = constructor_df.drop(constructor_df.url)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Add ingestion data & rename columns (one step)

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_constructor_df = drop_constructor_df.withColumnRenamed('constructorId','constructor_id') \
                                          .withColumnRenamed('constructorRef','constructor_ref') \
                                          .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to file

# COMMAND ----------

final_constructor_df.write.mode('overwrite').parquet('/mnt/formula123dl/processed/constructors')
