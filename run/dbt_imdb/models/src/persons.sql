
  
    
    

    create  table
      "imdb"."main"."persons__dbt_tmp"
  
    as (
      select 
    id,
    name
from "imdb"."main"."std_persons"
    );
  
  