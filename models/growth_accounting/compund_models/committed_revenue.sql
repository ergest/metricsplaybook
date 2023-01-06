{{
    config(materialized = 'view')
}}

select
    tr."month" as this_period,
    nr."month" + interval 1 month as next_period,
    tr.total_rr + nr.net_recurring_revenue 
from
    {{ ref('total_revenue') }} tr
    join {{ ref('net_revenue') }} nr on tr."month" = nr."month" + interval 1 month