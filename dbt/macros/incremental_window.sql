{#
    Filter predicate for incremental models.

    * When Airflow passes `interval_start` / `interval_end` (its data interval),
      only that window is rebuilt. Backfills of any past hour are therefore
      exact and can run in any order.
    * Otherwise (ad-hoc `dbt build`), reprocess from the latest hour already in
      the target table minus `lookback_hours`, to absorb late-arriving data.
#}
{% macro incremental_window(source_column, target_column=none) %}
    {%- set target_column = target_column or source_column -%}
    {%- if var('interval_start') and var('interval_end') -%}
        {{ source_column }} >= cast('{{ var("interval_start") }}' as timestamp)
        and {{ source_column }} < cast('{{ var("interval_end") }}' as timestamp)
    {%- else -%}
        {{ source_column }} >= (
            select {{ dbt.dateadd('hour', -1 * var('lookback_hours') | int, 'max(' ~ target_column ~ ')') }}
            from {{ this }}
        )
    {%- endif -%}
{% endmacro %}
