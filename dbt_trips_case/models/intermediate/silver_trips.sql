{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge'
    )
}}

with
    base_trips as (
        select *
        from {{ ref('stg_trips') }}

        -- Only load new trips after the last recorded event
        {% if is_incremental() %}
            where
                last_updated_timestamp > (
                    select coalesce(max(last_updated_timestamp), '1900-01-01')
                    from {{ this }}
                )
        {% endif %}
    ),

    transformed as (
        select
            trip_id,
            driver_id,
            customer_id,
            vehicle_id,
            -- Standardize location IDs (ensure no leading/trailing whitespace)
            trim(start_location) as start_location_id,
            trim(end_location) as end_location_id,
            -- Ensure numeric values are properly cast
            cast(distance_km as double) as distance_km,
            cast(fare_amount as double) as fare_amount,
            -- Keep timestamps
            trip_start_time,
            trip_end_time,
            trim(payment_method) as payment_method,
            trim(trip_status) as trip_status,
            last_updated_timestamp,
            ingested_at
        from base_trips
    )

-- Append new rows to the fact table
select *
from transformed
