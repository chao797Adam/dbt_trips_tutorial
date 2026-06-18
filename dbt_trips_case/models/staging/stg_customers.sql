{{
    config(
        materialized='incremental',
        unique_key='customer_id',
        incremental_strategy='merge'
    )
}}

select *, current_timestamp() as ingested_at
from {{ source('trips_source', 'customers') }}

{% if is_incremental() %}
    where last_updated_timestamp > (select max(last_updated_timestamp) from {{ this }})
{% endif %}
