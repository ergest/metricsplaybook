{{
    config(materialized = 'view')
}}

select
    month,
    1 - gcrcr as gdr
from
    {{ ref('gross_revenue_churn_rate') }}