{{-
    config(materialized = 'view')
-}}

with active_subscription as (
    select
        s.id as subscription_id,
        s.customer_id,
        s.started_at,
        si.mrr_value,
        lag(si.mrr_value, 1) over(partition by s.customer_id order by greatest(s.canceled_at, s.paused_at) desc) as previous_mrr
from
    {{ ref('subscription') }} s
    join {{ ref('subscription_item') }} si 
        on s.id = si.subscription_id
    where
        s.status = 'active'
)
select 
    date_trunc('month', started_at), sum(mrr_value) as resurrected_revenue
from
    active_subscription
where
    mrr_value > previous_mrr
group by 1