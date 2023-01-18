{{-
    config(materialized = 'table')
-}}

with total_rr_diff as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value as total_rr_current,
        lag(metric_value, 1) over(partition by slice_dimension order by metric_date) as total_rr_previous
    from
        {{ ref('total_revenue_cube')}}
)
select
    '{{ model.name }}' as metric_model,
    date_grain,
    metric_date,
    slice_dimension,
    slice_value,
    '1 - (total_rr(t) / total_rr(t-1)' as metric_calculation,
    1 - (total_rr_current / total_rr_previous) as metric_value
from
    total_rr_diff