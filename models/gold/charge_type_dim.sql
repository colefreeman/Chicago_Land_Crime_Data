{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH
src_crime_type_data_arrests as (
    SELECT
        DISTINCT("CHARGE_STATUTE") AS CHARGE_STATUTE
        , "CHARGE_DESCRIPTION"
        , "CHARGE_TYPE"
        , "CHARGE_CLASS"
    FROM {{ ref("silver_chicago_crimes_arrests") }}
)
SELECT
    ROW_NUMBER() OVER (ORDER BY "CHARGE_STATUTE") AS UNIQUE_ID
    , *
FROM src_crime_type_data_arrests