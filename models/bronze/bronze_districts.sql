{{ config(
    materialized='view',
    schema='BRONZE'
) }}

WITH
src_data_district as (
    SELECT
        "THE_GEOM" AS shape_map --Text
        , "DIST_NUM" AS district_number --Number
        , "DIST_LABEL" AS district_name --Number
        
    FROM {{source("seeds", "districts")}}
),
    hashed_beats_data as (
        SELECT
        {{ dbt_utils.generate_surrogate_key(["district_number"]) }} as districts_hkey
        , {{ dbt_utils.generate_surrogate_key(["district_number", "district_name"]) }} as districts_hdiff
        , *
        , '{{ run_started_at }}' as load_ts_utc
        FROM src_data_district
    )
SELECT * FROM hashed_beats_data