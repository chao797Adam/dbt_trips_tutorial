{{
    config(
        materialized='incremental',
        unique_key='trip_id',
        incremental_strategy='merge'
    )
}}

select *, current_timestamp() as ingested_at
from {{ source('trips_source', 'trips') }}

{% if is_incremental() %}
    where last_updated_timestamp > (select max(last_updated_timestamp) from {{ this }})
{% endif %}
