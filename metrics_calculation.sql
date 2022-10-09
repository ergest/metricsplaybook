/**
 * Metric: NewRR
 */
with contract_paid as (
    select
        customer_id,
        timestamp,
        activity,
        revenue_impact,
        dc.segment
    from
        contract_stream cs
        join dim_customer dc on cs.customer_id = dc.id 
    where
        activity = 'contract_paid'
        and activity_occurrence = 1
)
select
    date_trunc('month', timestamp) as month,
    sum(revenue_impact) as revenue
from
    contract_paid
group by 1
order by 1;

/**
 * Metric: ExpansionRR
 */
with contract_upgraded as (
    select
        customer_id,
        timestamp,
        activity,
        revenue_impact,
        dc.segment
    from
        contract_stream cs
        join dim_customer dc on cs.customer_id = dc.id 
    where
        activity = 'contract_upgraded'
)
select
    date_trunc('month', timestamp) as month,
    sum(revenue_impact) as revenue
from
    contract_paid
group by 1
order by 1;
