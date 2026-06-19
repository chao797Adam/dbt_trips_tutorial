{% snapshot trips_snapshot %}

    {{
    config(
        target_schema='snapshots',
        unique_key='trip_id',
        strategy='check',
        check_cols=['trip_status', 'trip_end_time', 'distance_km', 'fare_amount', 'payment_method']
    )
}}

    select *
    from {{ ref('stg_trips') }}

{% endsnapshot %}
