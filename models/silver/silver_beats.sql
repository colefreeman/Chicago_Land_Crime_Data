{{ config(
    materialized='view',
    schema='SILVER'
) }}

WITH
bronze_to_silver_beats as (
    SELECT
        *
    FROM {{ ref("bronze_beats") }}
)
SELECT 
    * EXCLUDE SHAPE_MAP
FROM bronze_to_silver_beats