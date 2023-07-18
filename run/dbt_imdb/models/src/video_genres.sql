
  
    
    

    create  table
      "imdb"."main"."video_genres__dbt_tmp"
  
    as (
      select  
    video_id,
    genre_id
from "imdb"."main"."std_movie_video_genres"
    );
  
  