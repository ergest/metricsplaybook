{{-
    config(materialized = 'table')
-}}

select
    '{{ model.name }}' as metric_model,
    tm.date_grain,
    tm.metric_date,
    tm.slice_dimension,
    tm.slice_value,
    '(total_rr(t) / total_rr(t-12)) ^ 1/12' as metric_calculation,
    power((tm.metric_value / lm.metric_value), 1.0/12.0) as metric_value
from
    {{ ref('total_revenue_cube')}} tm
    join{{ ref('total_revenue_cube')}} lm
        on tm.metric_date = lm.metric_date - interval 12 month
        and tm.slice_dimension = lm.slice_dimension
        and tm.date_grain = lm.date_grain