# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Load file & apply schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, FloatType

results_schema = StructType([StructField('resultsId', IntegerType(), False),\
                             StructField('raceId', IntegerType(), False),\
                             StructField('driverId', IntegerType(), False),\
                             StructField('constructorId', IntegerType(), False),\
                             StructField('number', IntegerType(), True),\
                             StructField('grid', IntegerType(), False),\
                             StructField('position', IntegerType(), True),\
                             StructField('positionText', StringType(), False),\
                             StructField('positionOrder', IntegerType(), False),\
                             StructField('points', FloatType(), False),\
                             StructField('laps', IntegerType(), False),\
                             StructField('time', StringType(), True),\
                             StructField('milliseconds', IntegerType(), True),\
                             StructField('fastestLap', IntegerType(), True),\
                             StructField('rank', IntegerType(), True),\
                             StructField('fastestLapTime', StringType(), True),\
                             StructField('fastestLapSpeed', StringType(), False),\
                             StructField('statusId', IntegerType(), False)])

# COMMAND ----------

results_df = spark.read \
    .schema(results_schema) \
    .json('/mnt/formula123dl/raw/results.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop, rename & add columns

# COMMAND ----------

from pyspark.sql.functions import col, lit, current_timestamp

transformed_results_df = results_df.drop(col('statusId')) \
                                   .withColumn('ingestion_date', current_timestamp()) \
                                   .withColumnRenamed('resultsId', 'results_id') \
                                   .withColumnRenamed('raceId', 'race_id') \
                                   .withColumnRenamed('driverId', 'driver_id') \
                                   .withColumnRenamed('constructorId', 'constructor_id') \
                                   .withColumnRenamed('positionText', 'position_text') \
                                   .withColumnRenamed('positionOrder', 'position_order') \
                                   .withColumnRenamed('fastestLap', 'fastest_lap') \
                                   .withColumnRenamed('fastestLapTime', 'fastest_lap_time') \
                                   .withColumnRenamed('fastestLapSpeed', 'fastest_lap_speed')

# COMMAND ----------

display(transformed_results_df)

# COMMAND ----------

transformed_results_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Export (partitioned by race_id)

# COMMAND ----------

final_results_df = transformed_results_df

# COMMAND ----------

final_results_df.write.partitionBy('race_id').parquet('/mnt/formula123dl/processed/results')
