
    
    

select
    video_id as unique_field,
    count(*) as n_records

from "imdb"."main"."videos"
where video_id is not null
group by video_id
having count(*) > 1


