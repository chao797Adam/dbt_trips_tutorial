{{
    config(
        materialized='incremental',
        unique_key='vehicle_id',
        incremental_strategy='merge'
    )
}}

with
    base_vehicles as (
        select *
        from {{ ref('stg_vehicles') }}

        -- Only process new or updated records during incremental runs
        {% if is_incremental() %}
            where
                last_updated_timestamp
                > (select max(last_updated_timestamp) from {{ this }})
        {% endif %}
    ),

    -- Deduplication logic: keep the latest record based on vehicle_id and timestamp
    deduplicated as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by vehicle_id order by last_updated_timestamp desc
                    ) as rn
                from base_vehicles
            )
        where rn = 1
    ),

    -- Transformation: clean and standardize vehicle data
    transformed as (
        select
            vehicle_id,
            trim(license_plate) as license_plate,
            trim(model) as model,
            upper(trim(make)) as make,
            year,
            trim(vehicle_type) as vehicle_type,
            last_updated_timestamp,
            ingested_at
        from deduplicated
    )

-- Final selection
select *
from transformed
