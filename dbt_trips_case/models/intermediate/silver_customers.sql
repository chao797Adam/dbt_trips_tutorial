{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

with
    base_customers as (
        select *
        from {{ ref('stg_customers') }}
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
                        partition by customer_id order by last_updated_timestamp desc
                    ) as rn
                from base_customers
            )
        where rn = 1
    ),

    transformed as (
        select
            *,
            split_part(email, '@', 2) as domain,
            regexp_replace(phone_number, '[^0-9]', '') as phone_number_clean,
            concat(first_name, ' ', last_name) as full_name
        from deduplicated
    )

select
    customer_id,
    full_name,
    email,
    domain,
    phone_number_clean as phone_number,
    city,
    signup_date,
    last_updated_timestamp,
    ingested_at
from transformed
