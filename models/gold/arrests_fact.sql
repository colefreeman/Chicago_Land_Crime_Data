{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH
arrests_fact AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY a."ARRESTS_HKEY") AS "UNIQUE_ID",
        a."ARRESTS_HKEY",
        a."ARRESTS_HDIFF",
        a."ARREST_DATE",
        s.SUSPECT_ID,
        c.CRIME_TYPE_ID,
        cn.CASE_NUMBER_ID
    FROM {{ ref("silver_chicago_crimes_arrests") }} a
    INNER JOIN {{ ref("dim_suspect") }} s ON a."CB_NUMBER" = s.CB_NUMBER
    INNER JOIN {{ ref("charge_type_dim") }} c ON a."CHARGE_STATUTE" = c.CHARGE_STATUTE
    INNER JOIN {{ ref("dim_case") }} cn ON a."CASE_NUMBER" = cn.CASE_NUMBER
)

SELECT * FROM arrests_fact