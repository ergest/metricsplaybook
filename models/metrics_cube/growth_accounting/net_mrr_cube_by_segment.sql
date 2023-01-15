{{-
    config(materialized = 'table')
-}}

with cte_new_mrr as (
    select
        series_month,
        segment as dimension,
        sum(revenue_impact) as recurring_revenue,
        count(distinct customer_id) as subscribers
    from
        {{ ref('new_mrr') }}
    group by 1,2
)
, cte_exapnsion_mrr as (
    select
        series_month,
        segment as dimension,
        sum(revenue_impact) as recurring_revenue,
        count(distinct customer_id) as subscribers
    from
        {{ ref('expansion_mrr') }} 
    group by 1,2
)
, cte_contraction_mrr as (
    select
        series_month,
        segment as dimension,
        sum(revenue_impact) as recurring_revenue,
        count(distinct customer_id) as subscribers
    from
        {{ ref('contraction_mrr') }} 
    group by 1,2
)
, cte_churned_mrr as (
    select
        series_month,
        segment as dimension,
        sum(revenue_impact) as recurring_revenue,
        count(distinct customer_id) as subscribers
    from
        {{ ref('churned_mrr') }} 
    group by 1,2
)
select
    coalesce(nr.series_month, er.series_month, cr.series_month, ch.series_month) as series_month,
    coalesce(nr.dimension, er.dimension, cr.dimension, ch.dimension) as dimension,
    sum(coalesce(nr.recurring_revenue, 0) + coalesce(er.recurring_revenue, 0) - coalesce(cr.recurring_revenue, 0) - coalesce(ch.recurring_revenue, 0)) as net_recurring_revenue,
    sum(coalesce(nr.subscribers, 0) + coalesce(er.subscribers, 0) - coalesce(cr.subscribers, 0) - coalesce(ch.subscribers, 0)) as net_subscribers
from
    cte_new_mrr nr
    full outer join cte_exapnsion_mrr er
        on nr.series_month = er.series_month
        and nr.dimension = er.dimension
    full outer join cte_contraction_mrr cr
        on nr.series_month = cr.series_month
        and nr.dimension = cr.dimension
    full outer join cte_churned_mrr ch
        on nr.series_month = ch.series_month
        and nr.dimension = ch.dimension
group by 1,2