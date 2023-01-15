{%-
  macro generate_net_mrr_cube (dimension)
-%}

with cte_new_mrr as (
    select
        date_grain,
        metric_date,
        metric_value,
        slice_dimension,
        slice_value
    from
        {{ ref('churned_mrr_cube') }}
    where
        slice_dimension = '{{ dimension }}'
)
, cte_exapnsion_mrr as (
    select
        date_grain,
        metric_date,
        metric_value,
        slice_dimension,
        slice_value
    from
        {{ ref('expansion_mrr_cube') }} 
    where
        slice_dimension = '{{ dimension }}'
)
, cte_contraction_mrr as (
    select
        date_grain,
        metric_date,
        metric_value,
        slice_dimension,
        slice_value
    from
        {{ ref('contraction_mrr_cube') }} 
    where
        slice_dimension = '{{ dimension }}'
)
, cte_churned_mrr as (
    select
        date_grain,
        metric_date,
        metric_value,
        slice_dimension,
        slice_value
    from
        {{ ref('churned_mrr_cube') }} 
    where
        slice_dimension = '{{ dimension }}'
)
select
    coalesce(nr.metric_date, er.metric_date, cr.metric_date, ch.metric_date) as metric_date,
    coalesce(nr.slice_value, er.slice_value, cr.slice_value, ch.slice_value) as slice_value,
    sum(coalesce(nr.metric_value, 0) + coalesce(er.metric_value, 0) - coalesce(cr.metric_value, 0) - coalesce(ch.metric_value, 0)) as net_recurring_revenue
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
group by 1,2

{% endmacro %}