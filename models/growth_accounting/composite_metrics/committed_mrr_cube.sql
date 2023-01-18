{{-
    config(materialized = 'table')
-}}

select
    '{{ model.name }}' as metric_model,
    tr.date_grain,
    metric_date,
    slice_dimension,
    slice_value,
    'tr.total_rr(t) + nr.net_recurring_revenue(t+1)' as metric_calculation,
    tr.metric_value + nr.metric_value
from
    {{ ref('total_mrr_cube') }} tr
    join {{ ref('net_mrr_cube') }} nr on tr.metric_date = nr.metric_date + interval 1 month