{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH
src_silver_gunshot_fact_data as (
    SELECT
        fact."GUNSHOTS_HKEY"
        , fact."GUNSHOTS_HDIFF"
        , fact."GUNSHOTS_ID"
        , fact."WARD"
        , fact."COMMUNITY_AREA_NUMBER"
        , fact."DISTRICT_NUMBER"
        , fact."BEAT_NUMBER"
        , fact."DATE_TIME"
        , mult.gunshot_type_id
        , loc.location_id
        , dates.DATE_ID
        , hours.hour_id
        , fact."NUMBER_ROUNDS_FIRED"
    FROM {{ ref("silver_chicago_crimes_gunshots") }} fact
    JOIN {{ ref("dim_date") }} dates ON fact."GUNSHOT_DATE" = dates."DATE_DAY"
    JOIN {{ ref("dim_hours") }} hours ON fact."HOUR_OF_GUNSHOT" = hours."daily_hour"
    JOIN {{ ref("single_mult_gunshot_dim") }} mult ON fact."SINGLE_MULTIPLE_GUNSHOTS" = mult.single_multiple_gunshot
    JOIN {{ ref("gunshot_location_dim") }} loc ON fact."LOCATION_ADDRESS" = loc.address
        AND fact."ZIP_CODE" = loc.zip
        AND fact."LAT" = loc.LAT
        AND fact."LNG" = loc.LNG
)
SELECT * FROM src_silver_gunshot_fact_data