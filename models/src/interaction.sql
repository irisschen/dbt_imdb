select
    user_id,
    item_id,
    interaction_type,
    start_time
from {{source('std', 'interaction_log')}}
where start_time >= '{{var("interaction_start_date")}}' and start_time <= '{{var("interaction_end_date")}}' and interaction_type in {{var('interaction_type')}}