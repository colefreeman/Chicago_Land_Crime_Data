{{ config(
    materialized='table',
    schema='GOLD'
) }}

SELECT * FROM {{ ref("silver_chicago_crimes_arrests") }}