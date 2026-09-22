{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge'
    )
}}

with
    raw_data as (
        select *, current_timestamp() as ingested_at
        from {{ source('trips_source', 'payments') }}

        {% if is_incremental() %}
            where
                last_updated_timestamp >= (
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
                        partition by payment_id
                        order by last_updated_timestamp desc, ingested_at desc
                    ) as rn
                from raw_data
            )
        where rn = 1
    )
select * except (rn)
from deduplicated
