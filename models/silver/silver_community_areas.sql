{{ config(
    materialized='view',
    schema='SILVER'
) }}

WITH
bronze_to_silver_community_areas as (
    SELECT
        *
    FROM {{ ref("bronze_community_areas") }}
)
SELECT 
    community_area_hkey
    , community_area_hdiff
    , community_area_number
    , community_name
    , load_ts_utc
FROM bronze_to_silver_community_areas