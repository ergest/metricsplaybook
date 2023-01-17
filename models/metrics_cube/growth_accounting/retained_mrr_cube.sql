{{-
    config(materialized = 'table')
-}}

with cte_prep as (
    select
        c.segment,
        c.channel,
        m.timestamp,
        m.revenue_impact,
        m.activity,
        m.plan_type
    from
        {{ ref('retained_mrr')}} m
        join {{ ref('dim_customer')}} c
            on m.customer_id = c.id
)
{{
    generate_metrics_cube (
        source_cte = 'cte_prep',
        anchor_date = 'timestamp',
        metric_calculation = 'sum(revenue_impact)',
        metric_slices = [
                ['segment'],
                ['channel'],
                ['plan_type']
        ],
        date_slices = ['month'],
        include_overall_total = true
    )
}}