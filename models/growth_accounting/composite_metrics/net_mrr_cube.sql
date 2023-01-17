{{-
    config(materialized = 'table')
-}}

with cte_new_mrr as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('new_mrr_cube') }}
)
, cte_exapnsion_mrr as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('expansion_mrr_cube') }} 
)
, cte_contraction_mrr as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('contraction_mrr_cube') }} 
)
, cte_churned_mrr as (
    select
        date_grain,
        metric_date,
        slice_dimension,
        slice_value,
        metric_value
    from
        {{ ref('churned_mrr_cube') }} 
)
select
    '{{ model.name }}' as metric_model,
    coalesce(nr.date_grain, er.date_grain, cr.date_grain, ch.date_grain) as date_grain,
    coalesce(nr.metric_date, er.metric_date, cr.metric_date, ch.metric_date) as metric_date,
    coalesce(nr.slice_dimension, er.slice_dimension, cr.slice_dimension, ch.slice_dimension) as slice_dimension,
    coalesce(nr.slice_value, er.slice_value, cr.slice_value, ch.slice_value) as slice_value,
    'new_mrr + expansion_mrr - contraction_mrr - churn_mrr' as metric_calculation,
    sum(coalesce(nr.metric_value, 0) + coalesce(er.metric_value, 0) - coalesce(cr.metric_value, 0) - coalesce(ch.metric_value, 0)) as metric_value
from
    cte_new_mrr nr
    full outer join cte_exapnsion_mrr er
        on nr.metric_date = er.metric_date
        and nr.slice_dimension = er.slice_dimension
    full outer join cte_contraction_mrr cr
        on nr.metric_date = cr.metric_date
        and nr.slice_dimension = cr.slice_dimension
    full outer join cte_churned_mrr ch
        on nr.metric_date = ch.metric_date
        and nr.slice_dimension = ch.slice_dimension
group by 1,2,3,4,5,6