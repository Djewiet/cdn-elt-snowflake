{{
    config(
        materialized='incremental',
        incremental_strategy='delete+insert',
        unique_key='request_id',
        on_schema_change='append_new_columns'
    )
}}

with

requests as (

    select * from {{ ref('int_cdn_requests_classified') }}
    {% if is_incremental() %}
        where {{ incremental_window('request_hour') }}
    {% endif %}

)

select
    -- keys
    request_id,
    {{ dbt_utils.generate_surrogate_key(['pop_code']) }} as pop_key,
    cast(
        extract(year from request_date) * 10000
        + extract(month from request_date) * 100
        + extract(day from request_date) as integer
    ) as date_key,
    http_status,

    -- time
    requested_at,
    request_hour,

    -- degenerate dimensions
    pop_code,
    client_country_code,
    http_method,
    host,
    request_path,
    content_category,
    device_type,
    cache_status,
    client_ip_hash,

    -- flags
    is_cacheable,
    is_cache_hit,
    is_client_error,
    is_server_error,

    -- measures
    bytes_sent,
    response_time_ms,

    loaded_at

from requests
