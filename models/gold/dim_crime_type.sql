{{ config(
    materialized='table',
    schema='GOLD'
) }}

SELECT
    "CRIME_TYPE"
    , ROW_NUMBER () OVER (ORDER BY "CRIME_TYPE") AS CRIME_TYPE_ID
FROM {{ ref("silver_chicago_crimes_crimes") }}
GROUP BY "CRIME_TYPE"
ORDER BY "CRIME_TYPE"