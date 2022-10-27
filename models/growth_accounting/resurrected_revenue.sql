{{
    config(materialized = 'view')
}}

select
    customer_id,
    timestamp,
    activity,
    revenue_impact
from
    contract_stream cs
where
    activity = 'resurrection_contract_started'
