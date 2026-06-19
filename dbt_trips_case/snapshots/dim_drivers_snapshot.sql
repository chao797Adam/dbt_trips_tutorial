{% snapshot drivers_snapshot %}

    {{
    config(
        target_schema='snapshots',
        unique_key='driver_id',
        strategy='check',
        check_cols=['phone_number', 'first_name', 'last_name']
    )
}}

    select *
    from {{ ref('stg_drivers') }}

{% endsnapshot %}
