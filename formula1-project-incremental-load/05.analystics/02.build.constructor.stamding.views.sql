-- Databricks notebook source
-- MAGIC %md
-- MAGIC Driver Build Standings
-- MAGIC - - Sources
-- MAGIC - 1.fact_session_results
-- MAGIC - 2.dim_drivers
-- MAGIC - - Output Column
-- MAGIC - -
-- MAGIC - 
-- MAGIC - season
-- MAGIC - driver id
-- MAGIC - driver name
-- MAGIC - nationality
-- MAGIC - race statrts
-- MAGIC - total points
-- MAGIC - number of wins
-- MAGIC mu,net of podium
-- MAGIC stamdimh position

-- COMMAND ----------

CREATE OR REPLACE VIEW formual1.gold.veiw_constructor_standing
AS 
WITH constructor_session_summary
AS(
SELECT 
    r.season,
    c.constructor_id,
    c.constructor_name,
    c.nationality,
    COUNT(*) AS race_starts,
    SUM(r.points) AS total_points,
    COUNT_IF(r.is_win) AS number_of_wins,
    COUNT_IF(r.is_podium) AS number_of_podiums
FROM formual1.gold.fact_session_results r 
JOIN formual1.gold.dim_constructors c  
    ON r.constructor_id = c.constructor_id
GROUP BY 
    r.season,
    c.constructor_id,
    c.constructor_name,
    c.nationality
)
SELECT season,
    constructor_id,
    constructor_name,
    nationality,
    RANK() OVER(PARTITION BY season ORDER BY total_points DESC, number_of_wins DESC) AS standing,
    race_starts,
    total_points,
    number_of_wins,
    number_of_podiums
FROM constructor_session_summary;


-- COMMAND ----------

SELECT * FROM formual1.gold.veiw_constructor_standing Where season = 2025