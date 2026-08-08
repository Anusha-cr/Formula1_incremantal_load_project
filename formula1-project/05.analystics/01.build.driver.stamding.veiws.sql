-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Driver Build Standings
-- MAGIC
-- MAGIC ## - Sources
-- MAGIC - 1.fact_session_results
-- MAGIC - 2.dim_drivers
-- MAGIC - 
-- MAGIC ## - Output Column
-- MAGIC - 
-- MAGIC - 1. season
-- MAGIC - 2. driver id
-- MAGIC - 3. driver name
-- MAGIC - 4. nationality
-- MAGIC - 5. race statrts
-- MAGIC - 6. total points
-- MAGIC - 7. number of wins
-- MAGIC - 8. mu,net of podium
-- MAGIC - 9. stamdimh position
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE VIEW formual1.gold.veiw_driver_standing
AS 
WITH driver_session_summary
AS(
SELECT 
    r.season,
    d.driver_id,
    d.driver_name,
    d.nationality,
    COUNT(*) AS race_starts,
    SUM(r.points) AS total_points,
    COUNT_IF(r.is_win) AS number_of_wins,
    COUNT_IF(r.is_podium) AS number_of_podiums
FROM formual1.gold.fact_session_results r 
JOIN formual1.gold.dim_drivers d   
    ON r.driver_id = d.driver_id
GROUP BY 
    r.season,
    d.driver_id,
    d.driver_name,
    d.nationality)
SELECT season,
    driver_id,
    driver_name,
    nationality,
    RANK() OVER(PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing,
    race_starts,
    total_points,
    number_of_wins,
    number_of_podiums
FROM driver_session_summary;



-- COMMAND ----------

SELECT * FROM formual1.gold.veiw_driver_standing Where season = 2025