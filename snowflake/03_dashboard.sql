-- =============================================================================
-- 03 — Dashboard: Streamlit in Snowflake, owned by the read-only CDN_REPORTER
-- Run once as ACCOUNTADMIN, then create the app from Snowsight with the
-- CDN_REPORTER role (see README, "Dashboard").
-- =============================================================================

use role accountadmin;

-- Where the app lives (kept out of the dbt-managed schemas)
create schema if not exists analytics.dashboards;

grant usage on schema analytics.dashboards to role cdn_reporter;
grant create streamlit, create stage on schema analytics.dashboards to role cdn_reporter;

-- Read access to the marts (future grants already cover tables dbt creates later)
grant usage on schema analytics.marts to role cdn_reporter;
grant select on all tables in schema analytics.marts to role cdn_reporter;
grant select on all views in schema analytics.marts to role cdn_reporter;

-- ACCOUNTADMIN already inherits CDN_REPORTER (through SYSADMIN). Another user
-- needs it granted explicitly:  grant role cdn_reporter to user <user_name>;
