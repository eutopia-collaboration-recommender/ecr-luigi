{% macro safe_make_date(input_year, input_month, input_day) %}
case
        -- Check if the date is valid using make_date
        when {{input_year}} is not null and {{input_month}} is not null and {{input_day}} is not null and
             make_date(greatest({{input_year}}::int, 1900), greatest({{input_month}}::int, 1), least({{input_day}}::int, extract(day from (make_date(greatest({{input_year}}::int, 1900), greatest({{input_month}}::int, 1), 1) + interval '1 month - 1 day')::date)::int)) is not null
        then
            -- Return the valid date using adjusted day if necessary
            make_date(
                greatest({{input_year}}::int, 1900),
                greatest({{input_month}}::int, 1),
                least({{input_day}}::int, extract(day from (make_date(greatest({{input_year}}::int, 1900), greatest({{input_month}}::int, 1), 1) + interval '1 month - 1 day')::date)::int)
            )
        else
            null
end
{% endmacro %}