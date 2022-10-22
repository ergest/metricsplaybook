with contract_signed as (
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
        activity = 'contract_signed'
        and activity_occurrence = 1
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
        activity = 'contract_upgraded'
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
        activity = 'contract_downgraded'
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
        activity = 'contract_churned'
)
select
    coalesce(date_trunc('month', cs.timestamp), date_trunc('month', cu.timestamp), date_trunc('month', cd.timestamp), date_trunc('month', cc.timestamp)) as month,
    sum(coalesce(cs.revenue_impact,0) + coalesce(cu.revenue_impact,0) - coalesce(cd.revenue_impact,0) - coalesce(cc.revenue_impact,0)) as pm_revenue
from
    contract_signed cs
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