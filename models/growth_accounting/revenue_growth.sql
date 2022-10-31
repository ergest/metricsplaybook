{{
    config(materialized = 'view')
}}

with total_rr_diff as (
    select
        month,
        total_rr as total_rr_current,
        lag(total_rr, 1) over(partition by month order by month) as total_rr_previous
    from
        {{ ref('total_revenue')}}
)
select
    month,
    1 - (total_rr_current - total_rr_previous) as growth_rate
from
    total_rr_diff