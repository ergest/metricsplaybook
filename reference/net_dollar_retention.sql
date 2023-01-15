{{
    config(materialized = 'view')
}}

select
    month,
    1 - nrcr as ndr
from
    {{ ref('net_revenue_churn_rate') }}