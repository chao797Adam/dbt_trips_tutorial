{% snapshot vehicles_snapshot %}

    {{
    config(
        target_schema='snapshots',
        unique_key='vehicle_id',
        strategy='check',
        check_cols=['model', 'make', 'year', 'vehicle_type']
    )
}}

    select *
    from {{ ref('stg_vehicles') }}

{% endsnapshot %}
