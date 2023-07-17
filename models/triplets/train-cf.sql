with test_interaction as (
    select
        user_id,
        item_id
    from {{ref('intersect')}}
    where start_time >= '{{ var("target_date") }}'
)

select distinct
    user_id,
    item_id
from {{ref('intersect')}}
where start_time < '{{ var("target_date") }}' and user_id in (select user_id from test_interaction)