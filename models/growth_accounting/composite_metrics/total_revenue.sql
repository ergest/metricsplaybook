{{
    config(materialized = 'view')
}}


select
    month,
    sum(net_recurring_revenue) over(partition by month order by month) as total_rr
from
    {{ ref('net_revenue') }}