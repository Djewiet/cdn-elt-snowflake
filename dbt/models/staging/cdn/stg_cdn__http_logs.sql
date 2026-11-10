with

source as (

    select * from {{ source('cdn', 'http_logs') }}

),

renamed as (

    select
        request_id,
        cast(event_ts as timestamp) as requested_at,
        upper(edge_pop) as pop_code,
        client_ip_hash,
        upper(client_country) as client_country_code,
        upper(http_method) as http_method,
        lower(host) as host,
        path as request_path,
        cast(http_status as integer) as http_status,
        cast(bytes_sent as bigint) as bytes_sent,
        cast(response_time_ms as double) as response_time_ms,
        upper(cache_status) as cache_status,
        lower(content_type) as content_type,
        user_agent,
        _source_file as source_file,
        cast(_loaded_at as timestamp) as loaded_at

    from source
    where request_id is not null

)

select * from renamed
-- An hour can be re-delivered (retry / backfill): keep the latest load only.
qualify row_number() over (partition by request_id order by loaded_at desc) = 1
