{{-
    config(materialized = 'table')
-}}

select
    '{{ model.name }}' as metric_model,
    cr.date_grain,
    cr.metric_date,
    cr.slice_dimension,
    cr.slice_value,
    'churned_rr + contraction_rr' as metric_calculation,
    cr.metric_value + ch.metric_value as metric_value
from
    {{ ref('contraction_revenue_cube') }} cr
    join {{ ref('churned_revenue_cube') }} ch
        on cr.metric_date = ch.metric_date
        and cr.slice_dimension = ch.slice_dimension
        and cr.date_grain = ch.date_grain
    left join {{ ref('total_revenue_cube') }} tr 
        on tr.metric_date = cr.metric_date - interval 1 month
        and tr.slice_dimension = cr.slice_dimension
        and tr.date_grain = cr.date_grain