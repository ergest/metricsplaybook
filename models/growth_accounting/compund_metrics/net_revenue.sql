{{
    config(materialized = 'view')
}}

select
    dt.series_month as month,
    sum(coalesce(nr.revenue_impact,0) + coalesce(er.revenue_impact,0) - coalesce(cr.revenue_impact,0) - coalesce(cxr.revenue_impact,0)) as net_recurring_revenue
from
    {{ ref('dim_date')}} dt
    left join {{ ref('new_mrr') }} nr
        on dt.series_month = date_trunc('month', nr.timestamp)
    left join {{ ref('expansion_mrr') }} er
        on date_trunc('month', nr.timestamp) = date_trunc('month', er.timestamp)
    left join {{ ref('contraction_mrr') }} cr
        on dt.series_month = date_trunc('month', cr.timestamp)
    left join {{ ref('churned_mrr') }} cxr
        on dt.series_month = date_trunc('month', cxr.timestamp)
group by 1