{#
  Generic test: the combination of columns is unique across the model.

    data_tests:
      - unique_combination_of_columns:
          combination_of_columns: [location_id, observed_at]
#}
{% test unique_combination_of_columns(model, combination_of_columns) %}

    {%- set cols = combination_of_columns | join(", ") -%}

    select {{ cols }}, count(*) as duplicate_count
    from {{ model }}
    group by {{ cols }}
    having count(*) > 1

{% endtest %}
