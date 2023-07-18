create or replace view "imdb"."main"."train-cf__dbt_int" as (
        select * from 'train-cf.tsv'
    );