-- not likely to change, but we want to track changes to location attributes over time
{% snapshot dim_locations_snapshot %}

    {{
    config(
        target_schema='snapshots',
        unique_key='location_id',
        strategy='check',
        check_cols=['city', 'state', 'country']
    )
}}

    select *
    from {{ ref('silver_locations') }}

{% endsnapshot %}
