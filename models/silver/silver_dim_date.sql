{{
    config(
        materialized = "view",
        schema='SILVER'
    )
}}

{{ dbt_date.get_date_dimension("1990-01-01", "2050-12-31") }}