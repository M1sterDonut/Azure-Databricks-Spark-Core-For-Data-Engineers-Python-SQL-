-- Databricks notebook source
SELECT driver_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points,
       RANK () OVER(ORDER BY AVG(calculated_points) DESC) AS driver_rank
  FROM f1_presentation.calculated_race_results
GROUP BY driver_name
HAVING total_races > 50
ORDER BY avg_points DESC 

-- COMMAND ----------

SELECT race_year,
       driver_name, 
       COUNT(1) AS total_races,
       SUM(calculated_points) AS total_points,
       AVG(calculated_points) AS avg_points
  FROM f1_presentation.calculated_race_results
GROUP BY race_year, driver_name
ORDER BY race_year, avg_points DESC 

-- COMMAND ----------


