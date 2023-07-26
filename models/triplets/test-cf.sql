select distinct
    user_id,
    item_id
from {{ref('intersect')}}
where start_time >= '{{ var("target_date") }}' and user_id in (select column0 from {{ref('train-cf')}})