# Databricks notebook source
# MAGIC %md
# MAGIC ### Step 1 - Ingest file & apply schema

# COMMAND ----------

#write schema
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, TimestampType

races_schema = StructType([StructField('raceId',IntegerType(), nullable=False), \
                          StructField('year',IntegerType(), nullable=True), \
                           StructField('round',IntegerType(), nullable=True), \
                           StructField('circuitId',IntegerType(), nullable=False), \
                           StructField('name',StringType(), nullable=True), \
                           StructField('date',StringType(), nullable=True), \
                           StructField('time',StringType(), nullable=True)])

# COMMAND ----------

#load file
races_df = spark.read \
    .schema(races_schema) \
    .option('header', True) \
    .csv('dbfs:/mnt/formula123dl/raw/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Drop unwanted column

# COMMAND ----------

from pyspark.sql.functions import col

selected_races_df = races_df.select(col('raceId'),col('year'),col('round'),col('circuitId'),col('name'),col('date'),col('time'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Rename columns

# COMMAND ----------

renamed_races_df = selected_races_df.withColumnRenamed('raceId','race_id') \
                                    .withColumnRenamed('year','race_year') \
                                    .withColumnRenamed('circuitID','circuit_id')

# COMMAND ----------

type(renamed_races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add column

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

ingestion_races_df = renamed_races_df.withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 - Transform column

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, concat

transform_races_df = ingestion_race_df.withColumn('race_timestamp', to_timestamp(concat(col('date'),lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# COMMAND ----------

final_races_df = transform_races_df.select(col('race_Id'),col('race_year'),col('round'),col('circuit_Id'),col('name'),col('race_timestamp'),col('ingestion_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 6 - Write to file

# COMMAND ----------

final_races_df.write.parquet('/mnt/formula123dl/processed/races', mode='overwrite')

# COMMAND ----------

df = spark.read.parquet('/mnt/formula123dl/processed/races')
