
  
    
    

    create  table
      "imdb"."main"."videos__dbt_tmp"
  
    as (
      select distinct 
    video_id
from "imdb"."main"."std_movie_video_content_ratings"
where content_rating_id NOT IN ('adult')
    );
  
  