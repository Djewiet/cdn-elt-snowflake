select
    {{ dbt_utils.generate_surrogate_key(['pop_code']) }} as pop_key,
    pop_code,
    pop_name,
    city,
    country_code,
    region,
    latitude,
    longitude,
    utc_offset_hours
from {{ ref('edge_pops') }}
