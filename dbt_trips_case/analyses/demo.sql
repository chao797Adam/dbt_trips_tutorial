select * from {{ source('trips_source', 'trips') }} limit 5
