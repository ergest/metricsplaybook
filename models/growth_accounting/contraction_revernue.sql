with contract_downgraded as (
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
)
select
    date_trunc('month', timestamp) as month,
    sum(revenue_impact) as revenue
from
    contract_downgraded
group by 1
order by 1;