select distinct
    user_id,
    item_id,
    start_time
from {{ref('interaction')}}
WHERE item_id in (select video_id from {{ ref('videos') }})