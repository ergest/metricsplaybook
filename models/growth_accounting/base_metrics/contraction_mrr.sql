{{-
    config(materialized = 'view')
-}}

select
    c.id as customer_id,
    c.segment,
    c.channel,
    c.cohort,
    m.timestamp,
    m.revenue_impact,
    m.activity,
    m.plan_type
from
    {{ ref('contract_stream') }} m
    join {{ ref('dim_customer')}} c
        on m.customer_id = c.id
where
    m.activity = 'contraction_contract_started'