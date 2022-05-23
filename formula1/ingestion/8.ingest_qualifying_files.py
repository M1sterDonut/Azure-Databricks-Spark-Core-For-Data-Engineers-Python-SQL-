# Databricks notebook source
#ingesting multiple json files

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Ingest five CSV files / lap_times folder

# COMMAND ----------

#define schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

qualifying_schema = StructType([StructField('qualifyId', IntegerType(), False),\
                              StructField('raceId', IntegerType(), False),\
                              StructField('driverId', IntegerType(), False),\
                              StructField('constructorId', IntegerType(), False),\
                              StructField('number', IntegerType(), False),\
                              StructField('position', IntegerType(), True),\
                              StructField('q1', StringType(), True),\
                              StructField('q2', StringType(), True),\
                              StructField('q3', StringType(), True)])

# COMMAND ----------

#ingest data
#can ingest entire folders or file names with wildcard (*) parts

qualifying_df = spark.read \
    .schema(qualifying_schema) \
    .option('multiLine',True) \
    .json(f'{raw_folder_path}/qualifying/qualifying_split_*.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

almost_final_qualifying_df = qualifying_df.withColumnRenamed('qualifyId','qualify_id') \
                                   .withColumnRenamed('constructorId', 'constructor_id') \
                                   .withColumnRenamed('driverId','driver_id') \
                                   .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

final_qualifying_df = add_ingestion_date(almost_final_qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write df into DeltaLake as parquet file

# COMMAND ----------

final_qualifying_df.write.mode('overwrite').parquet(f'{processed_folder_path}/qualifying')
