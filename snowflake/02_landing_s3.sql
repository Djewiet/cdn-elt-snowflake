-- =============================================================================
-- 02 — Landing: S3 storage integration, Parquet file format, stage, raw table
-- Run as ACCOUNTADMIN. Replace <BUCKET> and <AWS_ROLE_ARN>.
-- The integration avoids any AWS key in Snowflake: Snowflake assumes an IAM
-- role that only has s3:GetObject/ListBucket on the landing prefix.
-- =============================================================================

use role accountadmin;

create storage integration if not exists cdn_s3_int
    type = external_stage
    storage_provider = 's3'
    enabled = true
    storage_aws_role_arn = '<AWS_ROLE_ARN>'          -- e.g. arn:aws:iam::123456789012:role/snowflake-cdn-landing
    storage_allowed_locations = ('s3://<BUCKET>/cdn_logs/');

-- Copy STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID into the IAM
-- role's trust policy (README, step "S3 + IAM").
desc integration cdn_s3_int;

grant usage on integration cdn_s3_int to role cdn_loader;

-- ----------------------------------------------------------- loader objects
use role cdn_loader;
use warehouse cdn_wh;
use schema raw.cdn;

create file format if not exists parquet_ff
    type = parquet
    use_logical_type = true        -- load Parquet timestamps as TIMESTAMP, not epoch numbers
    binary_as_text = false;

create stage if not exists cdn_logs_stage
    url = 's3://<BUCKET>/cdn_logs/'
    storage_integration = cdn_s3_int
    file_format = parquet_ff
    comment = 'Hourly Parquet drops: dt=YYYY-MM-DD/hour=HH/cdn_logs_YYYYMMDDHH.parquet';

create table if not exists http_logs (
    request_id        varchar       not null,
    event_ts          timestamp_ntz not null,
    edge_pop          varchar,
    client_ip_hash    varchar,
    client_country    varchar,
    http_method       varchar,
    host              varchar,
    path              varchar,
    http_status       number(3, 0),
    bytes_sent        number(18, 0),
    response_time_ms  float,
    cache_status      varchar,
    content_type      varchar,
    user_agent        varchar,
    -- lineage columns populated by COPY INTO ... INCLUDE_METADATA
    _source_file      varchar,
    _loaded_at        timestamp_ltz
)
comment = 'Raw CDN access logs (1 row = 1 request). Loaded hourly by Airflow.';

-- transformer reads the table (the future grant only applies to tables created later)
grant select on table http_logs to role cdn_transformer;

-- Smoke test once the first file has landed:
-- list @cdn_logs_stage;
-- select * from @cdn_logs_stage (file_format => 'parquet_ff') limit 5;
