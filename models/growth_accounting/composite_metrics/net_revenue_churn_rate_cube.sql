{{-
    config(materialized = 'table')
-}}

with cte_churn_revenue as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('churned_revenue_cube') }}
),
cte_contraction_revenue as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('contraction_revenue_cube') }}
),
cte_expansion_revenue as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('expansion_revenue_cube') }}
)
select
    '{{ model.name }}' as metric_model,
    cr.date_grain,
    cr.metric_date,
    cr.slice_dimension,
    cr.slice_value,
    '(expansion_rr - churned_rr + contraction_rr) / total_rr' as metric_calculation,
    (er.metric_value - cr.metric_value - ch.metric_value) / nullif(tr.metric_value, 0) as metric_value
from
    cte_expansion_revenue er
    join cte_churn_revenue ch
        on er.metric_date = ch.metric_date
        and er.slice_dimension = ch.slice_dimension
    join cte_contraction_revenue cr
        on er.metric_date = cr.metric_date
        and er.slice_dimension = cr.slice_dimension
    left join {{ ref('total_revenue_cube') }} tr 
        on tr.metric_date = cr.metric_date - interval 1 month
        and tr.slice_dimension = cr.slice_dimension