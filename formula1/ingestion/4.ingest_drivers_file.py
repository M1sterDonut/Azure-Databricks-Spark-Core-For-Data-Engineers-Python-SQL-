# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Read file using Spark DataFrame API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType([StructField('forename', StringType(), nullable=False),\
                          StructField('surname', StringType(), nullable=False)])

# COMMAND ----------

driver_schema = StructType([StructField('driverId', IntegerType(), nullable=False),\
                            StructField('driverRef', IntegerType(), nullable=False),\
                            StructField('number', IntegerType(), nullable=True),\
                            StructField('code', IntegerType(), nullable=True),\
                            StructField('name', name_schema),\
                            StructField('dob', DateType(), nullable=True),\
                            StructField('nationality', StringType(), nullable=True),\
                            StructField('url', StringType(), nullable=False)])

# COMMAND ----------

drivers_df = spark.read \
    .schema(driver_schema) \
    .json(f'{raw_folder_path}/drivers.json')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Rename & add new columns
# MAGIC 1. rename driverId & driverRef
# MAGIC 2. add ingestion date
# MAGIC 3. concat fore and surname

# COMMAND ----------

from pyspark.sql.functions import concat, lit, col

almost_transformed_drivers_df = drivers_df.withColumnRenamed('driverId', 'driver_id') \
                                   .withColumnRenamed('driverRef', 'driver_ref') \
                                   .withColumn('name',concat(col('name.forename'), lit(' '), col('name.surname')))

# COMMAND ----------

transformed_drivers_df = add_ingestion_date(almost_transformed_drivers_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Drop unwanted columns

# COMMAND ----------

final_drivers_df = transformed_drivers_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Write to parquet

# COMMAND ----------

final_drivers_df.write \
                .mode('overwrite') \
                .parquet(f'{processed_folder_path}/drivers')
