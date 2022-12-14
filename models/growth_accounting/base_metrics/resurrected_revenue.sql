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
    dt.*
from
    {{ ref('contract_stream') }} m
    join {{ ref('dim_customer')}} c
        on m.customer_id = c.id
    join {{ ref('dim_date') }} dt
        on dt.series_day = date_trunc('day', m.timestamp)
where
    m.activity = 'resurrection_contract_started'
