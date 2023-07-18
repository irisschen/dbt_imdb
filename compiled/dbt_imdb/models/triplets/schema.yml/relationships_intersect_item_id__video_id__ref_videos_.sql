
    
    

with child as (
    select item_id as from_field
    from "imdb"."main"."intersect"
    where item_id is not null
),

parent as (
    select video_id as to_field
    from "imdb"."main"."videos"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


