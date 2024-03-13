{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH
src_silver_gunshot_type_data as (
    SELECT
        DISTINCT ("SINGLE_MULTIPLE_GUNSHOTS") as single_multiple_gunshot
        , ROW_NUMBER () OVER (ORDER BY single_multiple_gunshot) as gunshot_type_id
    FROM {{ ref("silver_chicago_crimes_gunshots") }}
    GROUP BY single_multiple_gunshot
)
SELECT * FROM src_silver_gunshot_type_data