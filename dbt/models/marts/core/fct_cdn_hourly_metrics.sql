{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key=['request_hour', 'pop_code', 'content_category'],
        on_schema_change='append_new_columns'
    )
}}

{#
    Grain: 1 row per hour x edge POP x content category.
    The KPIs a CDN team watches: offload (cache hit ratio), error rates,
    latency percentiles and egress volume.
#}

with

requests as (

    select * from {{ ref('fct_cdn_requests') }}
    {% if is_incremental() %}
        where {{ incremental_window('request_hour') }}
    {% endif %}

),

aggregated as (

    select
        request_hour,
        date_key,
        pop_key,
        pop_code,
        content_category,

        count(*) as request_count,
        sum(case when is_cacheable then 1 else 0 end) as cacheable_request_count,
        sum(case when is_cache_hit then 1 else 0 end) as cache_hit_count,
        sum(case when is_client_error then 1 else 0 end) as client_error_count,
        sum(case when is_server_error then 1 else 0 end) as server_error_count,
        count(distinct client_ip_hash) as unique_client_count,

        sum(bytes_sent) as bytes_served,
        sum(case when is_cache_hit then bytes_sent else 0 end) as bytes_served_from_cache,

        avg(response_time_ms) as avg_response_time_ms,
        percentile_cont(0.50) within group (order by response_time_ms) as p50_response_time_ms,
        percentile_cont(0.95) within group (order by response_time_ms) as p95_response_time_ms,
        percentile_cont(0.99) within group (order by response_time_ms) as p99_response_time_ms

    from requests
    group by 1, 2, 3, 4, 5

)

select
    {{ dbt_utils.generate_surrogate_key(['request_hour', 'pop_code', 'content_category']) }}
        as hourly_metric_key,
    *,
    cache_hit_count * 1.0 / nullif(cacheable_request_count, 0) as cache_hit_ratio,
    bytes_served_from_cache * 1.0 / nullif(bytes_served, 0) as byte_offload_ratio,
    (client_error_count + server_error_count) * 1.0 / request_count as error_rate,
    server_error_count * 1.0 / request_count as server_error_rate
from aggregated
