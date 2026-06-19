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
        {% if is_incremental() %}
            where
                last_updated_timestamp > (
                    select coalesce(max(last_updated_timestamp), '1900-01-01')
                    from {{ this }}
                )
        {% endif %}
    ),

    deduplicated as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by trip_id order by last_updated_timestamp desc
                    ) as rn
                from base_trips
            )
        where rn = 1
    ),

    transformed as (
        select
            trip_id,
            driver_id,
            customer_id,
            vehicle_id,
            trim(start_location) as start_location_id,
            trim(end_location) as end_location_id,
            cast(distance_km as double) as distance_km,
            cast(fare_amount as double) as fare_amount,
            trip_start_time,
            trip_end_time,
            trim(payment_method) as payment_method,
            trim(trip_status) as trip_status,
            last_updated_timestamp,
            ingested_at
        from deduplicated
    )

select *
from transformed
