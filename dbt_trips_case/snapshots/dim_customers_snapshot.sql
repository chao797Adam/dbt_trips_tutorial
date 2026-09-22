{% snapshot dim_customers_snapshot %}

    {{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['city', 'phone_number', 'full_name']
    )
}}

    select *
    from {{ ref('silver_customers') }}

{% endsnapshot %}
