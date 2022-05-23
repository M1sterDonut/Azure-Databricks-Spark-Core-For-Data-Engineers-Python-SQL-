# Databricks notebook source
#ingesting multiple files

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

lap_times_schema = StructType([StructField('raceId', IntegerType(), False),\
                              StructField('driverId', IntegerType(), False),\
                              StructField('lap', IntegerType(), False),\
                              StructField('position', IntegerType(), True),\
                              StructField('time', IntegerType(), True),\
                              StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

#ingest data
#can ingest entire folders or file names with wildcard (*) parts

lap_times_df = spark.read \
    .schema(lap_times_schema) \
    .csv(f'{raw_folder_path}/lap_times/lap_times_split*.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

almost_final_lap_times_df = lap_times_df.withColumnRenamed('driverId','driver_id') \
                                     .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

final_lap_times_df = add_ingestion_date(almost_final_lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write df into DeltaLake as parquet file

# COMMAND ----------

final_lap_times_df.write.mode('overwrite').parquet(f'{processed_folder_path}/lap_times')
