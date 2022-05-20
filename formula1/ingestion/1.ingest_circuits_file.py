# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file

# COMMAND ----------

#Read CSV file using Spark DataFrame reader

circuits_df = spark.read \
    .option("header", True) \
    .schema(circuit_schema) \
    .csv("dbfs:/mnt/formula123dl/raw/circuits.csv")

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

#Variable types

circuits_df.printSchema()
