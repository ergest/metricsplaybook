{{-
    config(materialized = 'table')
-}}

with cte_prep as (
    select
        c.id as customer_id,
        c.segment,
        c.channel,
        c.cohort,
        m.timestamp,
        m.revenue_impact,
        m.activity,
        m.plan_type
    from
        {{ ref('contract_stream') }} m
        join {{ ref('dim_customer')}} c
            on m.customer_id = c.id
    where
        m.activity = 'expansion_contract_started'
)
{{
    generate_metrics_cube (
        source_cte = 'cte_prep',
        anchor_date = 'timestamp',
        metric_calculation = 'count(customer_id)',
        metric_slices = [
                ['segment'],
                ['channel'],
                ['plan_type']
        ],
        date_slices = ['month'],
        include_overall_total = true
    )
}}