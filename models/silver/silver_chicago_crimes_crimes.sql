
{{ config(
    materialized='view',
    schema='SILVER'
) }}

WITH 
bronze_to_silver_crime_data as (
    SELECT 
    *
    , {{ to_timestamp("CRIME_DATE") }} as crime_date_time
FROM {{ ref("bronze_chicago_crimes_crimes") }}
),
date_hours as (
    SELECT
        *
        , date("CRIME_DATE_TIME") AS DATE_OF_CRIME
        ,{{ dbt_date.day_of_week("CRIME_DATE_TIME", isoweek=true) }} as day_of_week
        , DATE_PART('hour', CRIME_DATE_TIME) AS hour_of_day
    FROM bronze_to_silver_crime_data
)
SELECT * EXCLUDE CRIME_DATE
FROM date_hours