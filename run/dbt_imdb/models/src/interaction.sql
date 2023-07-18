
  
    
    

    create  table
      "imdb"."main"."interaction__dbt_tmp"
  
    as (
      select
    user_id,
    item_id,
    interaction_type,
    start_time
from read_csv_auto('data/interaction_log.csv')
where start_time >= '2023-06-07' and start_time <= '2023-06-09' and interaction_type in ('click', 'play', 'purchase')
    );
  
  