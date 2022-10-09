select
    customer_id,
    "timestamp",
    activity,
    revenue_impact,
    dc.segment
from
    contract_stream cs
    join dim_customer dc on cs.customer_id = dc.id 
where
    activity = 'contract_paid'
    and activity_occurrence = 1;