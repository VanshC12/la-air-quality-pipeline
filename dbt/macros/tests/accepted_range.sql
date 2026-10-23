{#
  Generic test: every non-null value of `column_name` lies within
  [min_value, max_value]. Either bound may be omitted.

    data_tests:
      - accepted_range:
          min_value: 0
          max_value: 1000
#}
{% test accepted_range(model, column_name, min_value=none, max_value=none) %}

    {%- set conditions = [] -%}
    {%- if min_value is not none -%}
        {%- do conditions.append(column_name ~ " < " ~ min_value) -%}
    {%- endif -%}
    {%- if max_value is not none -%}
        {%- do conditions.append(column_name ~ " > " ~ max_value) -%}
    {%- endif -%}
    {%- if conditions | length == 0 -%}
        {{ exceptions.raise_compiler_error("accepted_range needs min_value and/or max_value") }}
    {%- endif -%}

    select {{ column_name }} as offending_value
    from {{ model }}
    where {{ column_name }} is not null
      and ( {{ conditions | join(" or ") }} )

{% endtest %}
