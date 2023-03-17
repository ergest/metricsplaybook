{{-
    config(materialized = 'table')
-}}

with cte_prep as (
    select
        c.id as customer_id,
        c.cohort,
        c.first_touch_channel as channel,
        m.activity_ts,
        m.revenue_impact,
        m.activity,
        json_extract_string(m.feature_json, '$.segment') as segment,
        json_extract_string(m.feature_json, '$.plan') as plan
    from    
        {{ ref('customer_stream_started_subscription') }} m
        join {{ ref('customer')}} c
            on m.customer_id = c.id
    where
        m.activity = 'started_subscription'
)
{{
    generate_metrics_cube (
        source_cte = 'cte_prep',
        anchor_date = 'timestamp',
        metric_calculation = 'count(customer_id)',
        metric_slices = [
                ['segment'],
                ['channel'],
                ['plan']
        ],
        date_slices = ['month'],
        include_overall_total = true
    )
}}