

{{ config(
    materialized='table',
    schema='GOLD'
) }}

SELECT 
    DISTINCT "ADDRESS"
    , "LOCATION_DESCRIPTION"
    , "X_COORDINATE"
    , "Y_COORDINATE"
    , "LAT"
    , "LNG"
    , "COMBINED_COORDINATES"
    , ROW_NUMBER () OVER (ORDER BY "ADDRESS") AS CRIMES_LOC_ID
FROM {{ ref("silver_chicago_crimes_crimes") }}
