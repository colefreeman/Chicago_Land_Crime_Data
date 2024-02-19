{{ config(
    materialized='view',
    schema='BRONZE'
) }}
WITH
    src_data_arrests as (
        SELECT
            "cb_no" as cb_number
	        , "case_number" as case_number
	        , {{ to_timestamp("arrest_date") }} as arrest_date
	        , "race" as suspect_race
	        , "charge_1_statute" as charge_1_statute
	        , "charge_1_description" as charge_1_description
	        , "charge_1_type" as charge_1_type
	        , "charge_1_class" as charge_1_class
	        , "charge_2_statute" as charge_2_statute
	        , "charge_2_description" as charge_2_description
	        , "charge_2_type" as charge_2_type
	        , "charge_2_class" as charge_2_class
	        , "charge_3_statute" as charge_3_statute
	        , "charge_3_description" as charge_3_description
	        , "charge_3_type" as charge_3_type
	        , "charge_3_class" as charge_3_class
	        , "charge_4_statute" as charge_4_statute
	        , "charge_4_description" as charge_4_description
	        , "charge_4_type" as charge_4_type
	        , "charge_4_class" as charge_4_class
	        , "charges_statute" as charges_statute
	        , "charges_description" as charges_description
	        , "charges_type" as charges_type
	        , "charges_class" as charges_class

        FROM {{ source("chicago_crimes", "ARRESTS") }}
    ),
    hashed_arrest_data as (
        SELECT
        {{ dbt_utils.generate_surrogate_key(["cb_number", "case_number"]) }} as arrests_hkey
        , {{ dbt_utils.generate_surrogate_key(["case_number"]) }} as arrests_hdiff
        , *
        , '{{ run_started_at }}' as load_ts_utc
        FROM src_data_arrests
    )

SELECT * FROM hashed_arrest_data