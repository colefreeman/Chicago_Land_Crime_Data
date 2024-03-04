{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH
silver_person_arrests_src_data as (
    SELECT DISTINCT
        "CB_NUMBER" AS CB_NUMBER,
        "SUSPECT_RACE" AS SUSPECT_RACE
    FROM {{ ref("silver_chicago_crimes_arrests") }}
),
dim_suspects as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY "CB_NUMBER") AS SUSPECT_ID,
        CB_NUMBER,
        SUSPECT_RACE
    FROM silver_person_arrests_src_data
),
src_crime_type_data_arrests as (
    SELECT DISTINCT
        "CHARGE_STATUTE" AS CHARGE_STATUTE,
        "CHARGE_DESCRIPTION",
        "CHARGE_TYPE",
        "CHARGE_CLASS"
    FROM {{ ref("silver_chicago_crimes_arrests") }}
),
dim_crime_type as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY "CHARGE_STATUTE") AS CRIME_TYPE_ID,
        *
    FROM src_crime_type_data_arrests
),
src_case_number_silver_arrests_crimes as (
    SELECT DISTINCT
        "CASE_NUMBER"
    FROM {{ ref("silver_chicago_crimes_arrests") }}
    UNION
    SELECT DISTINCT
        "CASE_NUMBER"
    FROM {{ ref("silver_chicago_crimes_crimes") }}
),
dim_case_number as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY "CASE_NUMBER") AS "CASE_NUMBER_ID",
        "CASE_NUMBER"
    FROM src_case_number_silver_arrests_crimes
),
arrests_fact as (
    SELECT
        ROW_NUMBER() OVER (ORDER BY a.ARRESTS_HKEY) AS "UNIQUE_ID",
        a."ARRESTS_HKEY",
        a."ARRESTS_HDIFF",
        a."ARREST_DATE",
        s.SUSPECT_ID,
        c.CRIME_TYPE_ID,
        cn."CASE_NUMBER_ID"
    FROM {{ ref("silver_chicago_crimes_arrests") }} a
    INNER JOIN dim_suspects s ON s.CB_NUMBER = a.CB_NUMBER
    INNER JOIN dim_crime_type c ON c.CHARGE_STATUTE = a.CHARGE_STATUTE
    INNER JOIN dim_case_number cn ON cn.CASE_NUMBER = a.CASE_NUMBER
)
SELECT * FROM arrests_fact