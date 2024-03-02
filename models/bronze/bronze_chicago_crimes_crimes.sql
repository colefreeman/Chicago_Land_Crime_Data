{{ config(
    materialized='view',
    schema='BRONZE'
) }}

with
    src_data_crimes as (
        select
            "id" as crimes_id,  -- Number
            "case_number" as case_number,  -- TEXT
            "arrest_date" as crime_date, --DateTime 
            "block" as address,  -- TEXT
            "iucr" as iucr,  -- TEXT
            "primary_type" as crime_type,  -- TEXT
            "description" as crime_description,  -- TEXT
            "location_description" as location_description,  -- TEXT
            "arrest" as arrest_made,  -- BOOLEAN
            "beat" as beat,  -- NUMBER
            "district" as district,  -- NUMBER
            "ward" as ward,  -- NUMBER
            "community_area" as community_area,  -- NUMBER 
            "fbi_code" as fbi_code,  -- TEXT
            "x_coordinate" as x_coordinate,  -- NUMBER
            "y_coordinate" as y_coordinate,  -- NUMBER
            "arrest_year" as arrest_year,  -- NUMBER
            {{ to_timestamp("updated_on") }} as last_source_update,  -- DATE
            "lat" as lat,  -- NUMBER
            "lng" as lng,  -- NUMBER
            "location" as combined_coordinates,  -- TEXT
            'SOURCE_CRIME_DATA.CRIMES' as record_source

        from {{ source("chicago_crimes", "CRIMES") }}
    ),
    hashed_keys as (
        select
            {{ dbt_utils.generate_surrogate_key(["crimes_id", "case_number"]) }}
            as crimes_hkey,
            {{
                dbt_utils.generate_surrogate_key(
                    ["crimes_id", "case_number", "x_coordinate", "y_coordinate"]
                )
            }} as crimes_hdiff,
            *,
            '{{ run_started_at }}' as load_ts_utc
        from src_data_crimes
    )
select *
from hashed_keys
