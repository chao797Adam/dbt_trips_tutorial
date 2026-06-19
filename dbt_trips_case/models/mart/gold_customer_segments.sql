{{ config(materialized='table') }}

with
    trips as (select * from {{ ref('silver_trips') }}),
    customers as (select * from {{ ref('silver_customers') }})

select
    c.customer_id,
    c.full_name,
    c.city,
    count(t.trip_id) as lifetime_trips,
    sum(t.fare_amount) as lifetime_spend,
    case
        when count(t.trip_id) >= 5
        then 'Frequent'
        when count(t.trip_id) between 2 and 4
        then 'Regular'
        else 'Casual'
    end as customer_segment
from customers c
left join trips t on c.customer_id = t.customer_id
group by 1, 2, 3
