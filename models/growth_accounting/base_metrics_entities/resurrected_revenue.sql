{{-
    config(materialized = 'view')
-}}

with last_stopped_subscription as (
    select
        id as subscription_id,
        customer_id,
        greatest(canceled_at, paused_at) as stopped_at,
        row_number() over(partition by customer_id order by greatest(canceled_at, paused_at) desc) as rank
    from
        subscription
    where
        status in ('paused', 'canceled')
)
select 
    date_trunc('month', s.started_at), sum(si.mrr_value) as resurrected_revenue
from
    subscription s
    join subscription_item si
        on s.id = si.subscription_id
    join last_stopped_subscription lss
        on s.customer_id = lss.customer_id
        and datediff('day', lss.stopped_at, s.started_at) >= 30
where
    s.status = 'active'
    and lss.rank = 1
group by 1