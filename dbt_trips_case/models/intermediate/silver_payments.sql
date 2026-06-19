{{
    config(
        materialized='incremental',
        unique_key='payment_id',
        incremental_strategy='merge'
    )
}}

with
    base_payments as (
        select *
        from {{ ref('stg_payments') }}

        -- Only process new or updated records during incremental runs
        {% if is_incremental() %}
            where
                last_updated_timestamp > (
                    select coalesce(max(last_updated_timestamp), '1900-01-01')
                    from {{ this }}
                )
        {% endif %}
    ),

    -- Deduplication logic: keep the latest record based on payment_id and timestamp
    deduplicated as (
        select *
        from
            (
                select
                    *,
                    row_number() over (
                        partition by payment_id order by last_updated_timestamp desc
                    ) as rn
                from base_payments
            )
        where rn = 1
    ),

    -- Transformation: apply payment logic using CASE WHEN
    transformed as (
        select
            payment_id,
            trip_id,
            customer_id,
            payment_method,
            payment_status,
            amount,
            transaction_time,
            -- Mapping logic equivalent to PySpark when-otherwise
            case
                when payment_method = 'Card' and payment_status = 'Completed'
                then 'online_success'
                when payment_method = 'Card' and payment_status = 'Failed'
                then 'online_failed'
                when payment_method = 'Card' and payment_status = 'Pending'
                then 'online_pending'
                else 'offline'
            end as online_payment,
            last_updated_timestamp,
            ingested_at
        from deduplicated
    )

-- Final selection
select *
from transformed
