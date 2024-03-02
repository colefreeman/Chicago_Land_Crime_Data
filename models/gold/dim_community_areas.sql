{{ config(
    materialized='table',
    schema='GOLD'
) }}

SELECT *
FROM {{ ref("silver_community_areas") }}