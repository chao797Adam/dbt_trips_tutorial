{% snapshot dim_drivers_snapshot %}

    {{
    config(
        target_schema='snapshots',
        unique_key='driver_id',
        strategy='check',
        check_cols=['phone_number', 'full_name']
    )
}}

    select *
    -- changed
    from {{ ref('silver_drivers') }}

{% endsnapshot %}
