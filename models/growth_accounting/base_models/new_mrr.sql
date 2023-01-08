{{-
    config(materialized = 'view')
-}}

select
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
    activity = 'new_contract_started'