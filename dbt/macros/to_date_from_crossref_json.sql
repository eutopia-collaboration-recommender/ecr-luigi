{% macro to_date_from_crossref_json(json_column) %}
(
    CASE
    WHEN ({{ json_column }})::jsonb ? 'date-time' THEN
        (({{ json_column }})::jsonb ->> 'date-time')::timestamptz::date
    WHEN ({{ json_column }})::jsonb ? 'timestamp' THEN
        to_timestamp((( {{ json_column }})::jsonb ->> 'timestamp')::bigint / 1000)::date
    WHEN ({{ json_column }})::jsonb ? 'date-parts' THEN
        make_date(
            (({{ json_column }})::jsonb -> 'date-parts' -> 0 ->> 0)::int,
            (({{ json_column }})::jsonb -> 'date-parts' -> 0 ->> 1)::int,
            (({{ json_column }})::jsonb -> 'date-parts' -> 0 ->> 2)::int
        )
    ELSE
        NULL
    END
)
{% endmacro %}