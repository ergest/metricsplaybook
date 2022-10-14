/**
 * Metric: NewRR
 */
with customer_cohort as (
    select
        customer_id,
        timestamp as first_signed_date,
        date_trunc('month', timestamp) as first_signed_month,
        date_trunc('week', timestamp) as first_signed_week
    from
        contract_stream 
    where 
        activity = 'contract_signed'
        and activity_occurrence = 1
),
contract_paid as (
    select
        customer_id,
        timestamp,
        activity,
        revenue_impact,
        dc.segment
    from
        contract_stream cs
        join dim_customer dc on cs.customer_id = dc.id
        join customer_cohort cc on cs.customer_id = cs.customer_id
    where
        activity = 'contract_paid'
        and activity_occurrence = 1
);
--sample aggregation
/*
select
    date_trunc('month', timestamp) as month,
    sum(revenue_impact) as revenue
from
    contract_paid
group by 1
order by 1;
*/

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
    contract_upgraded
group by 1
order by 1;

/*
 * renewed = retaiend
 */