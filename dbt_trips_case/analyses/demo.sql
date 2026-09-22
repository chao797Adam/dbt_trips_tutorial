select * from {{ source('trips_source', 'customers') }}
