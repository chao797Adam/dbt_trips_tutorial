{{
    config(
        materialized='incremental',
        unique_key='location_id',
        incremental_strategy='merge'
    )
}}

with
    base_locations as (
        select *
        from {{ ref('stg_locations') }}

        -- Only process new or updated records during incremental runs
        {% if is_incremental() %}
            where
                last_updated_timestamp
                > (select max(last_updated_timestamp) from {{ this }})
        {% endif %}
    ),

    -- Deduplication logic: keep the latest record based on location_id and timestamp
    deduplicated as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by location_id order by last_updated_timestamp desc
                    ) as rn
                from base_locations
            )
        where rn = 1
    ),

    -- Data cleaning and transformation
    transformed as (
        select
            location_id,
            trim(city) as city,
            trim(state) as state,
            trim(country) as country,
            cast(latitude as double) as latitude,
            cast(longitude as double) as longitude,
            last_updated_timestamp,
            ingested_at
        from deduplicated
    )

-- Final selection
select *
from transformed
