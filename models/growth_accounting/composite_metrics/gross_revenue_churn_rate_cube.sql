{{-
    config(materialized = 'table')
-}}

with cte_churn as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('churned_revenue_cube') }}
),
cte_contraction as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('contraction_revenue_cube') }}
)
select
    '{{ model.name }}' as metric_model,
    cr.date_grain,
    cr.metric_date,
    cr.slice_dimension,
    cr.slice_value,
    '(churned_rr + contraction_rr) / total_rr' as metric_calculation,
    (cr.metric_value + ch.metric_value) / nullif(tr.metric_value, 0) as metric_value
from
    cte_contraction cr
    join cte_churn ch
        on cr.metric_date = ch.metric_date
        and cr.slice_dimension = ch.slice_dimension
    left join {{ ref('total_revenue_cube') }} tr 
        on tr.metric_date = cr.metric_date - interval 1 month
        and tr.slice_dimension = cr.slice_dimension