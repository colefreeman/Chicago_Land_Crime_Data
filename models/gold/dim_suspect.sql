{{ config(
    materialized='table',
    schema='GOLD'
) }}
-- Queries the necessary columns for the dim_suspects table
WITH
silver_arrests_src_data as (
    SELECT
        DISTINCT("CB_NUMBER") AS CB_NUMBER
        , "SUSPECT_RACE" AS SUSPECT_RACE
    FROM {{ ref("silver_chicago_crimes_arrests") }}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY "CB_NUMBER") AS UNIQUE_ID
    , CB_NUMBER
    , SUSPECT_RACE
FROM silver_arrests_src_data