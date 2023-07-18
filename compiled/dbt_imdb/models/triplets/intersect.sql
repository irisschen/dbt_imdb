select distinct
    user_id,
    item_id,
    start_time
from "imdb"."main"."interaction"
WHERE item_id in (select video_id from "imdb"."main"."videos")