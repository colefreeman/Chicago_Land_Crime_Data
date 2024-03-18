{{ config(
    materialized='table',
    schema='GOLD'
) }}

SELECT
    HOUR_OF_GUNSHOT AS "daily_hour"
    , ROW_NUMBER () OVER (ORDER BY "daily_hour") AS hour_id 
FROM {{ ref("silver_chicago_crimes_gunshots") }}
GROUP BY "daily_hour"