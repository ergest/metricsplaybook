{{
    config(materialized = 'view')
}}

select
    customer_id,
    timestamp,
    activity,
    revenue_impact
from
    {{ ref('raw_contract_stream') }}
where
    activity = 'contract_retained'