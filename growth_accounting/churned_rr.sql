/**
 * Metric: Churned Revenue
 * Assumptions: The contract_churned event is inserted with the appropriate negative amount
 */
with contract_churned as (
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
    date_trunc('month', timestamp) as month,
    -sum(revenue_impact) as revenue
from
    contract_churned
group by 1
order by 1;