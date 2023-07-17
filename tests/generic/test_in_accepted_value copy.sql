{% test in_accepted_value(model, column_name) %}

select
    {{ column_name }}
from {{ model }}
where {{ column_name }} not in {{ var(column_name) }}

{% endtest %}