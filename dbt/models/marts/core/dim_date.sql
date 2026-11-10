with

spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('" ~ var('date_spine_start') ~ "' as date)",
        end_date="cast('" ~ var('date_spine_end') ~ "' as date)"
    ) }}

),

days as (

    select cast(date_day as date) as date_day from spine

)

select
    cast(
        extract(year from date_day) * 10000
        + extract(month from date_day) * 100
        + extract(day from date_day) as integer
    ) as date_key,
    date_day,
    extract(year from date_day) as year_number,
    extract(quarter from date_day) as quarter_number,
    extract(month from date_day) as month_number,
    extract(day from date_day) as day_of_month,
    -- 0 = Sunday ... 6 = Saturday (same convention on Snowflake and DuckDB)
    extract(dayofweek from date_day) as day_of_week,
    case extract(dayofweek from date_day)
        when 0 then 'Sunday' when 1 then 'Monday' when 2 then 'Tuesday'
        when 3 then 'Wednesday' when 4 then 'Thursday' when 5 then 'Friday'
        else 'Saturday'
    end as day_name,
    extract(dayofweek from date_day) in (0, 6) as is_weekend,
    cast({{ dbt.date_trunc('month', 'date_day') }} as date) as month_start
from days
