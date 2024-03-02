{{ config(
    materialized='view',
    schema='BRONZE'
) }}

WITH
src_data_beats as (
    SELECT
        "THE_GEOM" AS shape_map --Text
        , "DISTRICT" AS district --Number
        , "SECTOR" AS sector --Number
        , "BEAT" AS beat --Number
        , "BEAT_NUM" AS beat_number --Number
        
    FROM {{source("seeds", "beats")}}
),
    hashed_beats_data as (
        SELECT
        {{ dbt_utils.generate_surrogate_key(["beat_number", "beat"]) }} as beats_hkey
        , {{ dbt_utils.generate_surrogate_key(["beat_number", "beat", "sector", "district"]) }} as beats_hdiff
        , *
        , '{{ run_started_at }}' as load_ts_utc
        FROM src_data_beats
    )
SELECT * FROM hashed_beats_data