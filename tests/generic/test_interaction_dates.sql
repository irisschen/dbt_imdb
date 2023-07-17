{% test interaction_dates(model, column_name) %}

select
    {{column_name}}
from {{ model }}
where {{column_name}} < '{{var("interaction_start_date")}}' and {{column_name}} > '{{var("interaction_end_date")}}'

{% endtest %}