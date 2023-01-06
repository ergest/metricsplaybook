{{
    config(materialized = 'view')
}}

select
    customer_id,
    timestamp,
    activity,
    revenue_impact
from
    {{ ref('contract_stream') }}
where
    activity = 'customer_churn_committed'
