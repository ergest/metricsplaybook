{{-
    config(materialized = 'table')
-}}

with total_subs_diff as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value as total_subs_current,
        lag(metric_value, 1) over(partition by slice_dimension order by metric_date) as total_subs_previous
    from
        {{ ref('total_subscriptions_cube')}}
)
select
    '{{ model.name }}' as metric_model,
    date_grain,
    metric_date,
    slice_dimension,
    slice_value,
    '1 - (total_subs(t) / total_subs(t-1)' as metric_calculation,
    1 - (total_subs_current / total_subs_previous) as metric_value
from
    total_subs_diff