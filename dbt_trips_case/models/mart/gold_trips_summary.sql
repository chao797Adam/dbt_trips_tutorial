{{ config(
    materialized='table'
) }}

with
    trips as (select * from {{ ref('silver_trips') }}),
    customers as (select * from {{ ref('silver_customers') }}),
    drivers as (select * from {{ ref('silver_drivers') }}),
    vehicles as (select * from {{ ref('silver_vehicles') }}),
    payments as (select * from {{ ref('silver_payments') }})

select
    t.trip_id,
    t.trip_start_time,
    t.trip_end_time,
    t.trip_status,
    t.fare_amount,
    t.distance_km,
    t.payment_method,
    -- customer info
    c.full_name as customer_name,
    c.email,
    c.city as customer_city,
    -- driver info
    d.full_name as driver_name,
    d.phone_number as driver_phone,
    d.driver_rating,
    d.vehicle_id,
    d.city as driver_city,
    -- vehicle info
    v.make,
    v.model,
    v.year as vehicle_year,
    v.license_plate,
    v.vehicle_type,
    -- Location data is excluded from join due to granularity mismatch:
    -- 'locations' is at city level, while 'trips' is at address/neighborhood level.
    -- Future mapping table is required for location enrichment.
    -- payment info
    p.payment_status,
    p.amount as payment_amount,
    p.transaction_time,
    p.online_payment
from trips t
left join customers c on t.customer_id = c.customer_id
left join drivers d on t.driver_id = d.driver_id
left join vehicles v on t.vehicle_id = v.vehicle_id
left join payments p on t.trip_id = p.trip_id
