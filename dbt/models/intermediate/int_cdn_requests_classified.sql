{#
    Business classification of each request: content category, client device,
    cache semantics and time buckets. Kept as a view so marts can rebuild any
    window cheaply; the heavy lifting is done incrementally downstream.
#}

with

requests as (

    select * from {{ ref('stg_cdn__http_logs') }}

),

classified as (

    select
        request_id,
        requested_at,
        {{ dbt.date_trunc('hour', 'requested_at') }} as request_hour,
        cast(requested_at as date) as request_date,
        pop_code,
        client_ip_hash,
        client_country_code,
        http_method,
        host,
        request_path,
        http_status,
        bytes_sent,
        response_time_ms,
        cache_status,
        content_type,

        case
            when content_type like 'image/%' then 'image'
            when content_type like 'video/%' then 'video'
            when content_type in ('text/javascript', 'application/javascript') then 'script'
            when content_type = 'text/css' then 'stylesheet'
            when content_type = 'application/json' then 'api'
            when content_type = 'text/html' then 'html'
            else 'other'
        end as content_category,

        case
            when lower(user_agent) like '%bot%'
                or lower(user_agent) like '%spider%'
                or lower(user_agent) like 'curl/%' then 'bot'
            when lower(user_agent) like '%mobile%'
                or lower(user_agent) like '%iphone%'
                or lower(user_agent) like '%android%' then 'mobile'
            when lower(user_agent) like 'mozilla/%' then 'desktop'
            else 'other'
        end as device_type,

        cache_status != 'BYPASS' as is_cacheable,
        cache_status = 'HIT' as is_cache_hit,
        http_status >= 400 and http_status < 500 as is_client_error,
        http_status >= 500 as is_server_error,
        loaded_at

    from requests

)

select * from classified
