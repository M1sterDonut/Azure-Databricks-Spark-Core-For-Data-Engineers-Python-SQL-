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

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ### Remove URL column
# MAGIC 
# MAGIC Two ways
# MAGIC * **select** only the columns you need
# MAGIC * **drop** the specific column

# COMMAND ----------

# MAGIC %md
# MAGIC **.select()** has **four styles:** \
# MAGIC \
# MAGIC 
# MAGIC **basic:** \
# MAGIC .select("columnName") \
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

display(circuits_selected_df)

# COMMAND ----------


