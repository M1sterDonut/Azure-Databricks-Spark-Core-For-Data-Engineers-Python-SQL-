# Databricks notebook source
#processing multi-line JSON file

# COMMAND ----------

# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read multi-line JSON file using Spark DF reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pitstop_schema = StructType([StructField('raceId', IntegerType(), False),\
                             StructField('driverId', IntegerType(), False),\
                             StructField('stop', IntegerType(), False),\
                             StructField('lap', IntegerType(), False),\
                             StructField('time', StringType(), False),\
                             StructField('duration', StringType(), True),\
                             StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

pitstops_df = spark.read \
    .schema(pitstop_schema) \
    .option('multiLine', True) \
    .json(f'{raw_folder_path}/pit_stops.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import col, lit

almost_final_pitstops_df = pitstops_df.withColumnRenamed('driverId','driver_id') \
                                     .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

final_pitstops_df = add_ingestion_date(almost_final_pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write df into DeltaLake as parquet file

# COMMAND ----------

final_pitstops_df.write.mode('overwrite').parquet(f'{processed_folder_path}/pitstops')
