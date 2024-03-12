{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH
silver_person_arrests_src_data as (
    SELECT 
        DISTINCT "CB_NUMBER" AS CB_NUMBER
        , "SUSPECT_RACE" AS SUSPECT_RACE
    FROM {{ ref("silver_chicago_crimes_arrests") }}
),
dim_suspects as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY "CB_NUMBER") AS SUSPECT_ID
        , CB_NUMBER
        , SUSPECT_RACE
    FROM silver_person_arrests_src_data
)
SELECT * FROM dim_suspects