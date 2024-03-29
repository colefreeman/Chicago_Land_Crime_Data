{{ config(
    materialized='view',
    schema='SILVER'
) }}

WITH
bronze_to_silver_gunshots_data as (
    SELECT 
        *
        , {{ to_timestamp("GUNSHOT_DATE_TIME") }} as date_time
        , date("GUNSHOT_DATE_TIME") as gunshot_date
        , {{ dbt_date.day_of_week("gunshot_date", isoweek=true) }} as day_of_week
        , DATE_PART('hour', date_time) AS hour_of_day
    FROM {{ ref("bronze_chicago_crimes_gunshots") }}
)
SELECT 
    * EXCLUDE(GUNSHOT_DATE_TIME, DAY_NUMBER_GUNSHOT, HOUR_OF_GUNSHOT)
FROM bronze_to_silver_gunshots_data