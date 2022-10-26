with contract_started as (
    select
        cs.customer_id,
        cs.timestamp,
        cs.activity,
        cs.revenue_impact,
        dc.segment,
        substring(date_trunc('month', dc.first_contract_signed_date)::text, 1, 7) as cohort
    from
        contract_stream cs
        join dim_customer dc on cs.customer_id = dc.id
    where
        activity = 'new_contract_started'
),
contract_upgraded as (
    select
        cs.customer_id,
        cs.timestamp,
        cs.activity,
        cs.revenue_impact,
        dc.segment,
        substring(date_trunc('month', dc.first_contract_signed_date)::text, 1, 7) as cohort
    from
        contract_stream cs
        join dim_customer dc on cs.customer_id = dc.id 
    where
        activity = 'expansion_contract_started'
),
contract_downgraded as (
    select
        cs.customer_id,
        cs.timestamp,
        cs.activity,
        cs.revenue_impact,
        dc.segment,
        substring(date_trunc('month', dc.first_contract_signed_date)::text, 1, 7) as cohort
    from
        contract_stream cs
        join dim_customer dc on cs.customer_id = dc.id 
    where
        activity = 'contraction_contract_started'
),
contract_churned as (
    select
        cs.customer_id,
        cs.timestamp,
        cs.activity,
        cs.revenue_impact,
        dc.segment,
        substring(date_trunc('month', dc.first_contract_signed_date)::text, 1, 7) as cohort
    from
        contract_stream cs
        join dim_customer dc on cs.customer_id = dc.id 
    where
        activity = 'customer_churn_committed'
),
net_revenue as (
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