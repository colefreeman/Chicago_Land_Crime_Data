{{ config(
    materialized='view',
    schema='BRONZE'
) }}

WITH
src_data_community_area as (
    SELECT
        "THE_GEOM" AS shape_map --Text
        , "PERIMETER" AS perimeter --Number
        , "AREA" AS area --Number
        , "COMAREA_" AS comarea --Number
        , "COMAREA_ID" AS comarea_id --Number
        , "AREA_NUMBE" AS community_area_number --Number
        , "COMMUNITY" AS community_name
        , "AREA_NUM_1" AS area_num_1 
        , "SHAPE_AREA" AS shape_area 
        , "SHAPE_LEN" AS shape_len
        
    FROM {{source("seeds", "community_areas")}}
),
    hashed_beats_data as (
        SELECT
        {{ dbt_utils.generate_surrogate_key(["community_area_number"]) }} as community_area_hkey
        , {{ dbt_utils.generate_surrogate_key(["community_area_number", "community_name"]) }} as community_area_hdiff
        , *
        , '{{ run_started_at }}' as load_ts_utc
        FROM src_data_community_area
    )
SELECT * FROM hashed_beats_data