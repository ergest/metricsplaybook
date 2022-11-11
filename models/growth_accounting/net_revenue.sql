{{
    config(materialized = 'view')
}}

select
    coalesce(date_trunc('month', nr.timestamp), date_trunc('month', er.timestamp), date_trunc('month', cr.timestamp), date_trunc('month', cxr.timestamp)) as month,
    sum(coalesce(nr.revenue_impact,0) + coalesce(er.revenue_impact,0) - coalesce(cr.revenue_impact,0) - coalesce(cxr.revenue_impact,0)) as net_recurring_revenue
from
    {{ ref('new_revenue') }} nr
    full outer join {{ ref('expansion_revenue') }} er
        and date_trunc('month', nr.timestamp) = date_trunc('month', er.timestamp)
    full outer join {{ ref('contraction_revenue') }} cr
        and date_trunc('month', nr.timestamp) = date_trunc('month', cr.timestamp)
    full outer join {{ ref('churned_revenue') }} cxr
        and date_trunc('month', nr.timestamp) = date_trunc('month', cxr.timestamp)
group by rollup?