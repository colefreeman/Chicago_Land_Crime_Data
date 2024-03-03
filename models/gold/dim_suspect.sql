{{ config(
    materialized='table',
    schema='GOLD'
) }}

-- queries the entire dataset to be broken apart for the fact table
WITH
silver_arrests_src_data as (
    SELECT
        DISTINCT("CB_NUMBER") AS CB_NUMBER
        , "SUSPECT_RACE" AS SUSPECT_RACE
    FROM {{ ref("bronze_chicago_crimes_arrests") }}
)
SELECT
    ROW_NUMBER() OVER (ORDER BY "CB_NUMBER") AS UNIQUE_ID
    , CB_NUMBER
    , SUSPECT_RACE
FROM silver_arrests_src_data