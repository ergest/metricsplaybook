{{-
    config(materialized = 'view')
-}}

select
    nr.series_month as month,
    sum(coalesce(nr.revenue_impact,0) + coalesce(er.revenue_impact,0) - coalesce(cr.revenue_impact,0) - coalesce(ch.revenue_impact,0)) as net_recurring_revenue
from
    {{ ref('new_mrr') }} nr
    full outer join {{ ref('expansion_mrr') }} er
        on nr.series_month = er.series_month
    full outer join {{ ref('contraction_mrr') }} cr
        on nr.series_month = cr.series_month
    full outer join {{ ref('churned_mrr') }} ch
        on nr.series_month = ch.series_month
group by 1