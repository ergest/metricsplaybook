{{
    config(materialized = 'view')
}}

with date_series as (
     select dd.generate_series as series_day
     from generate_series('2021-01-01'::timestamp, '2022-12-31'::timestamp, '1 day'::interval) dd
)
select
    series_day,
    date_trunc('month', series_day)::date   as series_month,
    date_trunc('week', series_day)::date    as series_week,
    date_trunc('quarter', series_day)::date as series_quarter,
    date_trunc('year', series_day)::date    as series_year
from
    date_series