select distinct start_location, length(start_location) as len
from {{ ref('silver_trips') }}
order by len desc
limit 20
;
