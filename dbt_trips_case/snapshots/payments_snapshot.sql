{% snapshot payments_snapshot %}

    {{
    config(
        target_schema='snapshots',
        unique_key='payment_id',
        strategy='check',
        check_cols=['payment_status', 'payment_method', 'amount']
    )
}}

    select *
    from {{ ref('stg_payments') }}

{% endsnapshot %}
