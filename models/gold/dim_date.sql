{{
    config(
        materialized = "table",
        schema='GOLD'
    )
}}

SELECT 
    *
    , ROW_NUMBER () OVER (ORDER BY "DATE_DAY") AS "DATE_ID"
FROM {{ ref("silver_dim_date") }}