{{ config(
    materialized='view',
    schema='SILVER'
) }}

WITH
    bronze_to_silver_arrests as (
        SELECT
        * EXCLUDE (CHARGES_STATUTE, CHARGES_DESCRIPTION, CHARGES_TYPE, CHARGES_CLASS)
        FROM {{ ref("bronze_chicago_crimes_arrests") }}  
),
    charge_1_columns as (
        SELECT
            "ARRESTS_HKEY"
            , "ARRESTS_HDIFF"
            , "CB_NUMBER"
            , "CASE_NUMBER"
            , "ARREST_DATE"
            , "SUSPECT_RACE"
            , "CHARGE_1_STATUTE" as charge_statute
            , "CHARGE_1_DESCRIPTION" as charge_description
            , "CHARGE_1_TYPE" as charge_type
            , "CHARGE_1_CLASS" as charge_class
            , "LOAD_TS_UTC" AS entry_timestamp
        FROM bronze_to_silver_arrests
    ),
        charge_2_columns as (
        SELECT
            "ARRESTS_HKEY"
            , "ARRESTS_HDIFF"
            , "CB_NUMBER"
            , "CASE_NUMBER"
            , "ARREST_DATE"
            , "SUSPECT_RACE"
            , "CHARGE_2_STATUTE" as charge_statute
            , "CHARGE_2_DESCRIPTION" as charge_description
            , "CHARGE_2_TYPE" as charge_type
            , "CHARGE_2_CLASS" as charge_class
            , "LOAD_TS_UTC" AS entry_timestamp
        FROM bronze_to_silver_arrests
    ),
        charge_3_columns as (
        SELECT
            "ARRESTS_HKEY"
            , "ARRESTS_HDIFF"
            , "CB_NUMBER"
            , "CASE_NUMBER"
            , "ARREST_DATE"
            , "SUSPECT_RACE"
            , "CHARGE_3_STATUTE" as charge_statute
            , "CHARGE_3_DESCRIPTION" as charge_description
            , "CHARGE_3_TYPE" as charge_type
            , "CHARGE_3_CLASS" as charge_class
            , "LOAD_TS_UTC" AS entry_timestamp
        FROM bronze_to_silver_arrests
    ),
        charge_4_columns as (
        SELECT
            "ARRESTS_HKEY"
            , "ARRESTS_HDIFF"
            , "CB_NUMBER"
            , "CASE_NUMBER"
            , "ARREST_DATE"
            , "SUSPECT_RACE"
            , "CHARGE_4_STATUTE" as charge_statute
            , "CHARGE_4_DESCRIPTION" as charge_description
            , "CHARGE_4_TYPE" as charge_type
            , "CHARGE_4_CLASS" as charge_class
            , "LOAD_TS_UTC" AS entry_timestamp
        FROM bronze_to_silver_arrests
    ),
    unioned_data as (
        SELECT * FROM charge_1_columns
        UNION ALL
        SELECT * FROM charge_2_columns
        UNION ALL
        SELECT * FROM charge_3_columns
        UNION ALL
        SELECT * FROM charge_4_columns 
    ),
    numbered_data AS (
        SELECT
            ROW_NUMBER() OVER (ORDER BY ARREST_DATE) AS unique_id
            , *
        FROM unioned_data
        WHERE charge_statute IS NOT NULL
    )

SELECT * 
FROM numbered_data