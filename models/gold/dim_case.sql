{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH
src_case_number_silver_arrests_crimes as (
    SELECT DISTINCT "CASE_NUMBER"
    FROM {{ ref("silver_chicago_crimes_arrests") }}
    UNION
    SELECT DISTINCT "CASE_NUMBER"
    FROM {{ ref("silver_chicago_crimes_crimes") }}
),
dim_case_number as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY "CASE_NUMBER") AS CASE_NUMBER_ID,
        "CASE_NUMBER" as CASE_NUMBER
    FROM src_case_number_silver_arrests_crimes
)
SELECT * FROM dim_case_number