{{ config(
    materialized='view',
    schema='BRONZE'
) }}

WITH 
src_data_gunshots as (
    SELECT
        "unique_id" AS GUNSHOTS_ID 
        , "date" as gunshot_date_time 
        , "block" AS LOCATION_ADDRESS
        , "zip_code" AS ZIP_CODE
        , "ward" AS WARD
        , "community_area" AS COMMUNITY_AREA
        , "area" AS COMMUNITY_AREA_NUMBER
        , "district" AS DISTRICT_NUMBER
        , "beat" AS BEAT_NUMBER
        , "street_outreach_organization" AS OUTREACH_ORG 
        , "month" AS MONTH_OF_GUNSHOT
        , "day_of_week" AS day_number_gunshot
        , "hour" AS HOUR_OF_GUNSHOT
        , "incident_type_description" AS SINGLE_MULTIPLE_GUNSHOTS
        , "rounds" AS NUMBER_ROUNDS_FIRED
        , "illinois_house_district" AS ILLINOIS_HOUSE_DISTRICT
        , "illinois_senate_district" AS ILLINOIS_SENATE_DISTRICT
        , "latitude" AS LAT
        , "longitude" AS LNG 
        , "location" AS COORDINATES
        , 'SOURCE_CRIME_DATA.GUNSHOTS' as record_source 
FROM {{ source("chicago_crimes", "GUNSHOTS") }}
),
hashed_gunshot_data as (
    SELECT
        {{ dbt_utils.generate_surrogate_key(["GUNSHOTS_ID"])  }} as gunshots_hkey
        , {{ dbt_utils.generate_surrogate_key(["GUNSHOTS_ID", "LAT", "LNG"]) }} as gunshots_hdiff
        , *
        , '{{ run_started_at }}' as load_ts_utc
FROM src_data_gunshots
)
SELECT * FROM hashed_gunshot_data