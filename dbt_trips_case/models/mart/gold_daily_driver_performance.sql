{{ config(materialized='table') }}

with
    trips as (select * from {{ ref('silver_trips') }}),
    drivers as (select * from {{ ref('silver_drivers') }})

select
    d.full_name as driver_name,
    date_trunc('day', t.trip_start_time) as trip_date,
    count(t.trip_id) as total_trips,
    sum(t.fare_amount) as total_revenue,
    avg(t.fare_amount) as avg_fare_per_trip
from trips t
left join drivers d on t.driver_id = d.driver_id
where t.trip_status = 'Completed'
group by
    1,
    2
    {# order by count(t.trip_id) desc, sum(t.fare_amount) desc #}
