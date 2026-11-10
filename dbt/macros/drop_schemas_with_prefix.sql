{#
    Drop every schema of the target database whose name starts with `prefix`.
    Used by CI to clean up the per-PR schemas (ci_pr_42, ci_pr_42_marts, ...).

        dbt run-operation drop_schemas_with_prefix --args "{prefix: 'ci_pr_42'}"
#}
{% macro drop_schemas_with_prefix(prefix) %}
    {% if not prefix or not prefix.startswith('ci_') %}
        {{ exceptions.raise_compiler_error("Refusing to drop schemas: prefix must start with 'ci_'") }}
    {% endif %}

    {% set schemas = run_query(
        "select schema_name from " ~ target.database ~ ".information_schema.schemata "
        ~ "where lower(schema_name) like lower('" ~ prefix ~ "%')"
    ).columns[0].values() %}

    {% for schema in schemas %}
        {% do log("Dropping schema " ~ target.database ~ "." ~ schema, info=true) %}
        {% do run_query("drop schema if exists " ~ target.database ~ "." ~ schema ~ " cascade") %}
    {% endfor %}
{% endmacro %}
