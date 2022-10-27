{{
    config(materialized = 'view')
}}

select
    customer_id,
    timestamp,
    activity,
    revenue_impact
from
    contract_stream
where
    activity = 'customer_churn_committed'