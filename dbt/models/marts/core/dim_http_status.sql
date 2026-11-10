select
    http_status,
    status_text,
    status_class,
    cast(is_error as boolean) as is_error,
    cast(is_server_error as boolean) as is_server_error
from {{ ref('http_status_codes') }}
