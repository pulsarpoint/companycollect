{% test company_serving_unique_key(model, key_columns) %}
SELECT {{ key_columns | join(', ') }}
FROM {{ model }}
GROUP BY {{ key_columns | join(', ') }}
HAVING count() > 1
{% endtest %}

{% test company_serving_sweden_anchor(model) %}
SELECT serving.company_id
FROM {{ model }} AS serving
LEFT JOIN {{ source('corpscout', 'se_companies') }} AS company FINAL
    ON company.company_id = serving.company_id
WHERE company.company_id = ''
GROUP BY serving.company_id
{% endtest %}
