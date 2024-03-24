{{ config(
    materialized='table',
    schema='GOLD'
) }}

WITH 
crimes_fact as (
    SELECT 
        fact."CRIMES_HKEY"
        , fact."CRIMES_HDIFF"
        , fact."CRIMES_ID"
        , "BEAT"
        , "DISTRICT"
        , "WARD"
        , "COMMUNITY_AREA"
        , "FBI_CODE"
        , "IUCR"
        , cases."CASE_NUMBER_ID"
        , type.CRIME_TYPE_ID
        , description.CRIME_DESC_ID
        , dates.DATE_ID
        , hours.hour_id
        , loc.CRIMES_LOC_ID
    FROM {{ ref("silver_chicago_crimes_crimes") }} AS fact
    JOIN {{ ref("dim_case") }} cases ON fact."CASE_NUMBER" = cases.CASE_NUMBER
    JOIN {{ ref("dim_crime_type") }} type ON fact."CRIME_TYPE" = type.CRIME_TYPE
    JOIN {{ ref("dim_crime_description") }} description ON fact."CRIME_DESCRIPTION" = description.CRIME_DESCRIPTION
    JOIN {{ ref("dim_date") }} dates ON fact."DATE_OF_CRIME" = dates.DATE_DAY
    JOIN {{ ref("dim_hours") }} hours ON fact."HOUR_OF_DAY" = hours."daily_hour"
    JOIN {{ ref("crimes_locations_dim") }} loc ON fact."ADDRESS" = loc.ADDRESS
        AND fact."LOCATION_DESCRIPTION" = loc.LOCATION_DESCRIPTION
        AND fact."X_COORDINATE" = loc.X_COORDINATE
        AND fact."Y_COORDINATE" = loc.Y_COORDINATE
        AND fact."LAT" = loc.LAT
        AND fact."LNG" = loc.LNG
        AND fact."COMBINED_COORDINATES" = loc.COMBINED_COORDINATES
)
SELECT 
    * 
    , ROW_NUMBER () OVER (ORDER BY CRIMES_ID) AS UNIQUE_ID
FROM crimes_fact