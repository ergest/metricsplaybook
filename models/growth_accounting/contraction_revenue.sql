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
    activity = 'contraction_contract_started'