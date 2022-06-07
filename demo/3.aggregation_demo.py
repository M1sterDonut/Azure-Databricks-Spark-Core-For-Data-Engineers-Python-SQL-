# Databricks notebook source
### Simple Aggregate functions

#sum, count, min, max (via .select())
#output: single result with agg applied

# COMMAND ----------

#%run (to get file path available)
#load data

# COMMAND ----------

#import functions you need
from pyspark.sql.functions import count, countDistinct, sum

# COMMAND ----------

demo_df.select(count("race_names")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_names")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter(demo_df["driver_name"] == 'Lewis Hamilton') \
       .select(sum("points"), countDistinct("race_name"))

# COMMAND ----------

### GroupBy functions

# COMMAND ----------

demo_grouped_df = demo_df.groupBy("driver_name")
       .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
       .show()

# COMMAND ----------

### Window functions

# COMMAND ----------

#function - rank

# COMMAND ----------

# 1) import Window and rank
from spark.sql.window import Window
from spark.sql.functions import rank, desc

# COMMAND ----------

# 2) creat window specification
driverRankSpec = Window.partionBy("race_year").orderBy(desc("total_points"))

# COMMAND ----------

# 3) apply window specification to dataframe
demo_grouped_df.withColumn("rank", rank().over(driverRankSpec))

# COMMAND ----------

### count if

# COMMAND ----------

count(when(col("position") == 1, True)).alias("wins")
