{#
    Daily health scorecard per edge POP.

    The SLO is evaluated per hour x POP (all content categories together):
    evaluating it at the hourly-metrics grain would let tiny groups
    (e.g. 1 error out of 7 video requests) dominate. Ratios are always
    recomputed from summed counts, never averaged.
#}

with

pop_hours as (

    select
        request_hour,
        date_key,
        pop_key,
        count(*) as request_count,
        sum(case when is_cacheable then 1 else 0 end) as cacheable_request_count,
        sum(case when is_cache_hit then 1 else 0 end) as cache_hit_count,
        sum(case when is_server_error then 1 else 0 end) as server_error_count,
        sum(bytes_sent) as bytes_served,
        sum(case when is_cache_hit then bytes_sent else 0 end) as bytes_served_from_cache,
        percentile_cont(0.95) within group (order by response_time_ms) as p95_response_time_ms
    from {{ ref('fct_cdn_requests') }}
    group by 1, 2, 3

),

daily as (

    select
        date_key,
        pop_key,
        count(*) as hours_observed,
        sum(request_count) as request_count,
        sum(cacheable_request_count) as cacheable_request_count,
        sum(cache_hit_count) as cache_hit_count,
        sum(server_error_count) as server_error_count,
        sum(bytes_served) as bytes_served,
        sum(bytes_served_from_cache) as bytes_served_from_cache,
        max(p95_response_time_ms) as worst_hourly_p95_ms,
        max(server_error_count * 1.0 / request_count) as worst_hourly_server_error_rate,
        sum(
            case
                when server_error_count * 1.0 / request_count >= {{ var('slo_max_server_error_rate', 0.01) }}
                    or p95_response_time_ms >= {{ var('slo_max_p95_ms', 500) }}
                    then 1
                else 0
            end
        ) as hours_breaching_slo
    from pop_hours
    group by 1, 2

)

select
    d.date_day,
    p.pop_code,
    p.pop_name,
    p.region,
    daily.hours_observed,
    daily.request_count,
    round(daily.bytes_served / power(1024, 3), 3) as gib_served,
    daily.cache_hit_count * 1.0 / nullif(daily.cacheable_request_count, 0) as cache_hit_ratio,
    daily.bytes_served_from_cache * 1.0 / nullif(daily.bytes_served, 0) as byte_offload_ratio,
    daily.server_error_count * 1.0 / daily.request_count as server_error_rate,
    daily.worst_hourly_p95_ms,
    daily.worst_hourly_server_error_rate,
    daily.hours_breaching_slo,
    -- SLO: on every hour of the day, 5xx rate < 1% and p95 latency < 500 ms
    daily.hours_breaching_slo = 0 as is_slo_met
from daily
inner join {{ ref('dim_date') }} as d on daily.date_key = d.date_key
inner join {{ ref('dim_edge_pop') }} as p on daily.pop_key = p.pop_key
