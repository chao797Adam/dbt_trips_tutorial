{{
    config(
        materialized='incremental',
        unique_key='vehicle_id',
        incremental_strategy='merge'
    )
}}

select *, current_timestamp() as ingested_at
from {{ source('trips_source', 'vehicles') }}

{% if is_incremental() %}
    where last_updated_timestamp > (select max(last_updated_timestamp) from {{ this }})
{% endif %}
