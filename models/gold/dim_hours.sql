{{ config(
    materialized='table',
    schema='GOLD'
) }}

SELECT
    hour_of_day AS "daily_hour"
    , ROW_NUMBER () OVER (ORDER BY "daily_hour") AS hour_id 
FROM {{ ref("silver_chicago_crimes_gunshots") }}
GROUP BY "daily_hour"