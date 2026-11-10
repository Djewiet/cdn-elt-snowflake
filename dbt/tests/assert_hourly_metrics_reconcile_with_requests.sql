-- The hourly snapshot must account for every request in the transaction fact.
-- Returns the hours where totals diverge (test fails if any row is returned).

with

requests as (

    select
        request_hour,
        count(*) as request_count,
        sum(bytes_sent) as bytes_served
    from {{ ref('fct_cdn_requests') }}
    group by 1

),

hourly as (

    select
        request_hour,
        sum(request_count) as request_count,
        sum(bytes_served) as bytes_served
    from {{ ref('fct_cdn_hourly_metrics') }}
    group by 1

)

select
    coalesce(r.request_hour, h.request_hour) as request_hour,
    r.request_count as fact_requests,
    h.request_count as hourly_requests
from requests as r
full outer join hourly as h on r.request_hour = h.request_hour
where r.request_count is null
    or h.request_count is null
    or r.request_count != h.request_count
    or r.bytes_served != h.bytes_served
