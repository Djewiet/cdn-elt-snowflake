-- =============================================================================
-- 01 — Account setup: warehouse, databases, RBAC, service users, cost guardrail
-- Run once as ACCOUNTADMIN (Snowsight worksheet or `snow sql -f`).
-- Replace the two <RSA_PUBLIC_KEY_...> placeholders (see README "Key pair").
-- =============================================================================

use role accountadmin;

-- ---------------------------------------------------------------- cost guard
-- Trial accounts have limited credits: hard-stop the warehouse at 10 credits/month.
create resource monitor if not exists cdn_rm
    with credit_quota = 10
    frequency = monthly
    start_timestamp = immediately
    triggers on 80 percent do notify
             on 100 percent do suspend;

create warehouse if not exists cdn_wh
    warehouse_size = xsmall
    auto_suspend = 60
    auto_resume = true
    initially_suspended = true
    resource_monitor = cdn_rm
    comment = 'CDN ELT pipeline (load + transform)';

-- ------------------------------------------------------------------ storage
create database if not exists raw comment = 'Landing zone: data as delivered, never mutated by dbt';
create schema if not exists raw.cdn;

create database if not exists analytics comment = 'dbt-managed models (staging, intermediate, marts)';

-- -------------------------------------------------------------------- roles
use role securityadmin;

create role if not exists cdn_loader comment = 'Airflow: writes to RAW.CDN';
create role if not exists cdn_transformer comment = 'dbt: reads RAW, owns ANALYTICS';
create role if not exists cdn_reporter comment = 'BI / analysts: read-only on marts';

grant role cdn_loader to role sysadmin;
grant role cdn_transformer to role sysadmin;
grant role cdn_reporter to role sysadmin;

-- warehouse
grant usage on warehouse cdn_wh to role cdn_loader;
grant usage on warehouse cdn_wh to role cdn_transformer;
grant usage on warehouse cdn_wh to role cdn_reporter;

-- loader: RAW.CDN read/write
grant usage on database raw to role cdn_loader;
grant usage, create table, create stage, create file format on schema raw.cdn to role cdn_loader;
grant select, insert, delete on future tables in schema raw.cdn to role cdn_loader;

-- transformer: read RAW, build everything in ANALYTICS
grant usage on database raw to role cdn_transformer;
grant usage on schema raw.cdn to role cdn_transformer;
grant select on future tables in schema raw.cdn to role cdn_transformer;
grant all privileges on database analytics to role cdn_transformer;

-- reporter: marts only (schemas created by dbt; future grants cover them)
grant usage on database analytics to role cdn_reporter;
grant usage on future schemas in database analytics to role cdn_reporter;
grant select on future tables in database analytics to role cdn_reporter;
grant select on future views in database analytics to role cdn_reporter;

-- ------------------------------------------------------- service users (keypair)
create user if not exists svc_cdn_loader
    type = service
    default_role = cdn_loader
    default_warehouse = cdn_wh
    rsa_public_key = '<RSA_PUBLIC_KEY_LOADER>'
    comment = 'Airflow loader';

create user if not exists svc_cdn_transformer
    type = service
    default_role = cdn_transformer
    default_warehouse = cdn_wh
    rsa_public_key = '<RSA_PUBLIC_KEY_TRANSFORMER>'
    comment = 'dbt (Airflow + GitHub Actions)';

-- Timestamps are stored and compared in UTC end to end.
alter user svc_cdn_loader set timezone = 'UTC';
alter user svc_cdn_transformer set timezone = 'UTC';

grant role cdn_loader to user svc_cdn_loader;
grant role cdn_transformer to user svc_cdn_transformer;
