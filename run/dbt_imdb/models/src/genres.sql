
  
    
    

    create  table
      "imdb"."main"."genres__dbt_tmp"
  
    as (
      select
    id,
    name
from "imdb"."main"."std_genres"
    );
  
  