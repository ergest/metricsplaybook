{{-
    config(materialized = 'view')
-}}

select
    nr.series_month,
    sum(coalesce(nr.revenue_impact,0) + coalesce(er.revenue_impact,0) - coalesce(cr.revenue_impact,0) - coalesce(ch.revenue_impact,0)) as net_recurring_revenue
from
    {{ ref('new_mrr') }} nr
    full outer join {{ ref('expansion_mrr') }} er
        on nr.series_month = er.series_month
        and nr.customer_id = er.customer_id
    full outer join {{ ref('contraction_mrr') }} cr
        on nr.series_month = cr.series_month
        and nr.customer_id = cr.customer_id
    full outer join {{ ref('churned_mrr') }} ch
        on nr.series_month = ch.series_month
        and nr.customer_id = ch.customer_id
group by 1