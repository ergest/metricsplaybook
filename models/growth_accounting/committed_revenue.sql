{{
    config(materialized = 'view')
}}

with net_revenue as (
    select
        coalesce(date_trunc('month', nr.timestamp), date_trunc('month', er.timestamp), date_trunc('month', cr.timestamp), date_trunc('month', cxr.timestamp)) as month,
        sum(coalesce(nr.revenue_impact,0) + coalesce(er.revenue_impact,0) - coalesce(cr.revenue_impact,0) - coalesce(cxr.revenue_impact,0)) as net_recurring_revenue
    from
        {{ ref('new_revenue') }} nr
        full outer join {{ ref('expansion_revenue') }} er
            on nr.customer_id = er.customer_id
            and date_trunc('month', nr.timestamp) = date_trunc('month', er.timestamp)
        full outer join {{ ref('contraction_revenue') }} cr
            on nr.customer_id = cr.customer_id
            and date_trunc('month', nr.timestamp) = date_trunc('month', cr.timestamp)
        full outer join {{ ref('churned_revenue') }} cxr
            on nr.customer_id = cxr.customer_id
            and date_trunc('month', nr.timestamp) = date_trunc('month', cxr.timestamp)
    group by 1
)
select
    month,
    sum(net_recurring_revenue) over(partition by month order by month) as total_rr
from
    net_revenue