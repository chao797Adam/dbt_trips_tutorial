{{
    config(
        materialized='incremental',
        unique_key='driver_id',
        incremental_strategy='merge'
    )
}}

select *, current_timestamp() as ingested_at
from {{ source('trips_source', 'drivers') }}

{% if is_incremental() %}
    where
        last_updated_timestamp
        > (select coalesce(max(last_updated_timestamp), '1900-01-01') from {{ this }})
{% endif %}
