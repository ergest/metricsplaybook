/**
 * Metric: Churned Revenue
 * Assumptions: The contract_churned event is inserted with the appropriate negative amount
 */
with contract_signed as (
    select
        cs.customer_id,
        cs.timestamp as this_contract_signed_ts,
        cs.activity_repeated_at as next_contract_signed_ts,
        cs.activity,
        cs.revenue_impact,
        dc.segment,
        substring(date_trunc('month', dc.first_contract_signed_date)::text, 1, 7) as cohort
    from
        contract_stream cs
        join dim_customer dc on cs.customer_id = dc.id
    where
        activity = 'contract_signed'
),
last_contract_churned as (
    select
        cs.customer_id,
        max(cs.timestamp) as last_contract_churned_ts
    from
        contract_stream cs
    where
        activity = 'contract_churned'
    group by 1
)
--sample aggregation
select
    date_trunc('month', this_contract_signed_ts) as month,
    sum(revenue_impact) as revenue
from
    last_contract_churned lc
    join contract_signed cs
        on lc.customer_id = cs.customer_id
        and cs.this_contract_signed_ts > lc.last_contract_churned_ts
group by 1
order by 1;
