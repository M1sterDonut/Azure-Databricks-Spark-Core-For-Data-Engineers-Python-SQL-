# Databricks notebook source
#ingesting multiple files

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Ingest five CSV files / lap_times folder

# COMMAND ----------

# ap_times table
# +--------------+--------------+------+-----+---------+-------+-----------------------------------+
# | Field        | Type         | Null | Key | Default | Extra | Description                       |
# +--------------+--------------+------+-----+---------+-------+-----------------------------------+
# | raceId       | int(11)      | NO   | PRI | NULL    |       | Foreign key link to races table   |
# | driverId     | int(11)      | NO   | PRI | NULL    |       | Foreign key link to drivers table |
# | lap          | int(11)      | NO   | PRI | NULL    |       | Lap number                        |
# | position     | int(11)      | YES  |     | NULL    |       | Driver race position              |
# | time         | varchar(255) | YES  |     | NULL    |       | Lap time e.g. "1:43.762"          |
# | milliseconds | int(11)      | YES  |     | NULL    |       | Lap time in milliseconds          |

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
    .csv('/mnt/formula123dl/raw/lap_times/lap_times_split*.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename & add new columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, col, lit

final_lap_times_df = lap_times_df.withColumn('ingestion_date', current_timestamp()) \
                                     .withColumnRenamed('driverId','driver_id') \
                                     .withColumnRenamed('raceId', 'race_id')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Write df into DeltaLake as parquet file

# COMMAND ----------

final_lap_times_df.write.mode('overwrite').parquet('/mnt/formula123dl/processed/lap_times')
