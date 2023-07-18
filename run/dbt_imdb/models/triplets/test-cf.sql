create or replace view "imdb"."main"."test-cf__dbt_int" as (
        select * from 'test-cf.tsv'
    );