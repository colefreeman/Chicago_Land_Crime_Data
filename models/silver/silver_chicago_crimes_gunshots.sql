{{ config(
    materialized='view',
    schema='SILVER'
) }}

WITH
bronze_to_silver_gunshots_data as (
    SELECT 
        *
        , {{ to_timestamp("GUNSHOT_DATE_TIME") }} as date_time  
    FROM {{ ref("bronze_chicago_crimes_gunshots") }}
)
SELECT 
    * EXCLUDE GUNSHOT_DATE_TIME
FROM bronze_to_silver_gunshots_data