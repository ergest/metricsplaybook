{{-
    config(materialized = 'table')
-}}

select
    metric_date,
    
    sum(net_recurring_revenue) over(partition by slice_value order by month) as total_mrr
from
    {{ ref('mrr_by_month') }}