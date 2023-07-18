
  
    
    

    create  table
      "imdb"."main"."video_persons__dbt_tmp"
  
    as (
      select
    video_id,
    person_id
from "imdb"."main"."std_movie_video_persons"
where video_id in (select video_id from "imdb"."main"."videos")
    );
  
  