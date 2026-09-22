{{ config(
    materialized='table',
    tags=['gold', 'aggregate']
) }}

with
    trips as (select * from {{ ref('silver_trips') }}),
    drivers as (select * from {{ ref('silver_drivers') }})

select
    t.driver_id,
    d.full_name as driver_name,
    date_trunc('day', t.trip_start_time) as trip_date,
    t.trip_status,
    count(t.trip_id) as total_trips,
    coalesce(sum(t.fare_amount), 0) as total_revenue,
    avg(t.fare_amount) as avg_fare_per_trip
from trips t
left join drivers d on t.driver_id = d.driver_id
where t.trip_start_time is not null
group by 1, 2, 3, 4
