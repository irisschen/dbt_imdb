

select
    interaction_type
from "imdb"."main"."interaction"
where interaction_type not in ('click', 'play', 'purchase')

