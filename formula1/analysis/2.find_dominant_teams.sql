-- Databricks notebook source
SELECT CURRENT_DATABASE()

-- COMMAND ----------

SELECT team_name,
       COUNT(1) AS race_number,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM calculated_race_results
GROUP BY team_name
HAVING race_number > 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT team_name,
       COUNT(1) AS race_number,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM calculated_race_results
  WHERE race_year BETWEEN 2011 AND 2020
GROUP BY team_name
HAVING race_number > 100
ORDER BY avg_points DESC

-- COMMAND ----------

SELECT team_name,
       COUNT(1) AS race_number,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM calculated_race_results
  WHERE race_year BETWEEN 2001 AND 2010
GROUP BY team_name
HAVING race_number > 100
ORDER BY avg_points DESC

-- COMMAND ----------


