{{
    config(
        materialized='incremental',
        unique_key='driver_id',
        incremental_strategy='merge'
    )
}}

with
    base_drivers as (
        select *
        from {{ ref('stg_drivers') }}

        -- Only process new or updated records during incremental runs
        {% if is_incremental() %}
            where
                last_updated_timestamp > (
                    select coalesce(max(last_updated_timestamp), '1900-01-01')
                    from {{ this }}
                )
        {% endif %}
    ),

    -- 1. Deduplication logic: keep the latest record based on driver_id and timestamp
    deduplicated as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by driver_id order by last_updated_timestamp desc
                    ) as rn
                from base_drivers
            )
        where rn = 1
    ),

    -- 2. Data transformation: clean phone number and concatenate names
    transformed as (
        select
            driver_id,
            concat(first_name, ' ', last_name) as full_name,
            regexp_replace(phone_number, '[^0-9]', '') as phone_number,
            vehicle_id,
            driver_rating,
            city,
            last_updated_timestamp,
            ingested_at
        from deduplicated
    )

-- 3. Final selection
select *
from transformed
