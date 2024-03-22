{{ config(
    materialized='table',
    schema='GOLD'
) }}

SELECT
    "CRIME_DESCRIPTION"
    , ROW_NUMBER () OVER (ORDER BY "CRIME_DESCRIPTION") AS CRIME_DESC_ID
FROM {{ ref("silver_chicago_crimes_crimes") }}
GROUP BY "CRIME_DESCRIPTION"
ORDER BY "CRIME_DESCRIPTION"