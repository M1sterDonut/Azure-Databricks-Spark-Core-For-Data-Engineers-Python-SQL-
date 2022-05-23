# Databricks notebook source
# MAGIC %run ../includes/configuration

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Ingest circuits.csv file

# COMMAND ----------

#inferSchema not efficient, because Spark has to read data twice
#for production enviornment, define your schema and apply to the file to import

#to specify schema, need to understand data types

#import types
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuit_schema = StructType([StructField("circuitId", IntegerType(), False), \
                             StructField("circuitRef", StringType(), True), \
                             StructField("name", StringType(), True), \
                             StructField("location", StringType(), True), \
                             StructField("lat", DoubleType(), True), \
                             StructField("lng", DoubleType(), True), \
                             StructField("alt", IntegerType(), True), \
                             StructField("url", StringType(), True)])

# COMMAND ----------

#Read CSV file using Spark DataFrame reader
circuits_df = spark.read \
    .option("header", True) \
    .schema(circuit_schema) \
    .csv(f"{raw_folder_path}/circuits.csv")

# COMMAND ----------

#Variable types
circuits_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 2 - Remove URL column
# MAGIC 
# MAGIC Two ways
# MAGIC * **select** only the columns you need
# MAGIC * **drop** the specific column

# COMMAND ----------

# MAGIC %md
# MAGIC **.select()** has **four styles:** 
# MAGIC 
# MAGIC 
# MAGIC **basic:** \
# MAGIC .select("columnName") 
# MAGIC 
# MAGIC **apply column-based functions:** \
# MAGIC .select(dfName.columnName) \
# MAGIC .select(dfName["columnName"]) \
# MAGIC .select(col("columnName"))

# COMMAND ----------

circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "lat", "lng", "alt")

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 3 - Rename column
# MAGIC 
# MAGIC can use two methods: 
# MAGIC 
# MAGIC 
# MAGIC 1. df.select(col("columnName").alias("newName")) 
# MAGIC 
# MAGIC OR 
# MAGIC \
# MAGIC 2. df.withColumnRenamed('oldName', 'newName').collect()

# COMMAND ----------

circuits_renamed_df = circuits_selected_df \
    .withColumnRenamed("circuitRef", "circuit_ref") \
    .withColumnRenamed("lat", "latitude") \
    .withColumnRenamed("lng", "longitude") \
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Add column that holds current timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Step 5 - Write to parquet file in DeltaLake

# COMMAND ----------

circuits_final_df.write \
    .mode("overwrite") \
    .parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

#read the data back
df = spark.read.parquet(f"{processed_folder_path}/circuits")
