{{
    config(materialized = 'view')
}}

with net_revenue as (
    select
        coalesce(date_trunc('month', cs.timestamp), date_trunc('month', cu.timestamp), date_trunc('month', cd.timestamp), date_trunc('month', cc.timestamp)) as month,
        sum(coalesce(cs.revenue_impact,0) + coalesce(cu.revenue_impact,0) - coalesce(cd.revenue_impact,0) - coalesce(cc.revenue_impact,0)) as net_recurring_revenue
    from
        contract_started cs
        full outer join contract_upgraded cu 
            on cs.customer_id = cu.customer_id
            and date_trunc('month', cs.timestamp) = date_trunc('month', cu.timestamp)
        full outer join contract_downgraded cd
            on cs.customer_id = cd.customer_id
            and date_trunc('month', cs.timestamp) = date_trunc('month', cd.timestamp)
        full outer join contract_churned cc
            on cs.customer_id = cc.customer_id
            and date_trunc('month', cs.timestamp) = date_trunc('month', cc.timestamp)
    group by 1
)
select
    month,
    sum(net_recurring_revenue) over(partition by month order by month) as total_rr
from
    net_revenue