{{ config(
    materialized='view',
    schema='SILVER'
) }}

WITH
bronze_to_silver_districts as (
    SELECT
        *
    FROM {{ ref("bronze_districts") }}
)
SELECT 
    * EXCLUDE SHAPE_MAP
FROM bronze_to_silver_districts