{{-
    config(materialized = 'table')
-}}

with cte_new_revenue as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('new_revenue_cube') }}
)
, cte_exapnsion_revenue as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('expansion_revenue_cube') }} 
)
, cte_contraction_revenue as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('contraction_revenue_cube') }} 
)
, cte_churned_revenue as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('churned_revenue_cube') }} 
)
select
    '{{ model.name }}' as metric_model,
    coalesce(nr.date_grain, er.date_grain, cr.date_grain, ch.date_grain) as date_grain,
    coalesce(nr.metric_date, er.metric_date, cr.metric_date, ch.metric_date) as metric_date,
    coalesce(nr.slice_dimension, er.slice_dimension, cr.slice_dimension, ch.slice_dimension) as slice_dimension,
    coalesce(nr.slice_value, er.slice_value, cr.slice_value, ch.slice_value) as slice_value,
    'new_rr + expansion_rr - contraction_rr - churn_rr' as metric_calculation,
    sum(coalesce(nr.metric_value, 0) + coalesce(er.metric_value, 0) - coalesce(cr.metric_value, 0) - coalesce(ch.metric_value, 0)) as metric_value
from
    cte_new_revenue nr
    full outer join cte_exapnsion_revenue er
        on nr.metric_date = er.metric_date
        and nr.slice_dimension = er.slice_dimension
    full outer join cte_contraction_revenue cr
        on nr.metric_date = cr.metric_date
        and nr.slice_dimension = cr.slice_dimension
    full outer join cte_churned_revenue ch
        on nr.metric_date = ch.metric_date
        and nr.slice_dimension = ch.slice_dimension
group by 1,2,3,4,5,6