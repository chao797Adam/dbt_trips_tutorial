select * from {{ source('trips_source', 'vehicles') }}
