{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH 
src_silver_gunshot_location_data as (
    SELECT
        DISTINCT ("LOCATION_ADDRESS") AS address
        , "ZIP_CODE" AS zip
        , "LAT"
        , "LNG"
        , "COORDINATES"
        , ROW_NUMBER () OVER (ORDER BY "ADDRESS", "ZIP_CODE", "LAT", "LNG") AS location_id
    FROM {{ ref("silver_chicago_crimes_gunshots") }}
)
SELECT * FROM src_silver_gunshot_location_data