{{
    config(materialized = 'view')
}}

with churn as (
    select
        coalesce(date_trunc('month', timestamp)) as month,
        sum(revenue_impact) as churned_rr
    from
        {{ ref('churned_revenue') }}
    group by 1
),
contraction as (
    select
        coalesce(date_trunc('month', timestamp)) as month,
        sum(revenue_impact) as contraction_rr
    from
        {{ ref('contraction_revenue') }}
    group by 1
),
expansion as (
    select
        coalesce(date_trunc('month', timestamp)) as month,
        sum(revenue_impact) as expansion_rr
    from
        {{ ref('expansion_revenue') }}
    group by 1
)
select
    cr.month as this_period,
    (coalesce(exp.expansion_rr, 0) - coalesce(chr.churned_rr, 0) - coalesce(cr.contraction_rr, 0)) / trr.total_rr as nrcr
from
    contraction cr
    join churn chr on cr.month = chr.month
    join expansion exp on cr.month = exp.month
    join {{ ref('total_revenue') }} trr on trr.month = cr.month - interval 1 month