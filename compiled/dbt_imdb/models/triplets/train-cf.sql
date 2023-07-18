with test_interaction as (
    select
        user_id,
        item_id
    from "imdb"."main"."intersect"
    where start_time >= '2023-06-09 00:00:00'
)

select distinct
    user_id,
    item_id
from "imdb"."main"."intersect"
where start_time < '2023-06-09 00:00:00' and user_id in (select user_id from test_interaction)