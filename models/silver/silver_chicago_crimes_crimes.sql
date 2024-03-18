
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
)
SELECT
    * EXCLUDE "CRIME_DATE"
FROM bronze_to_silver_crime_data