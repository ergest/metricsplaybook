{{-
    config(materialized = 'view')
-}}

select 
    date_trunc('month', started_at), sum(si.mrr_value) as new_revenue
from 
    subscription s
    join subscription_item si on s.id = si.subscription_id
where
    s.status = 'active'
    and not exists (
            select *
            from subscription
            where customer_id = s.customer_id
                and started_at < s.started_at
    )
group by 1