{{-
    config(materialized = 'table')
-}}

select
    '{{ model.name }}' as metric_model,
    tr.date_grain,
    tr.metric_date,
    tr.slice_dimension,
    tr.slice_value,
    'tr.total_recurring_revenue(t) + nr.net_recurring_revenue(t+1)' as metric_calculation,
    tr.metric_value + coalesce(nr.metric_value, 0)
from
    {{ ref('total_revenue_cube') }} tr
    left join {{ ref('net_revenue_cube') }} nr 
        on tr.metric_date = nr.metric_date + interval 1 month
        and nr.slice_dimension = tr.slice_dimension