{{-
    config(materialized = 'table')
-}}

with cte_grouping_sets as (
    select
        date_trunc('month', timestamp)::date as metric_month,
        grouping(metric_month) as month_bit,
        segment as dimension_1,
        channel as dimension_2,
        cohort  as dimension_3,
        'Total' as total_object,
        grouping(dimension_1) as dimension_1_bit,
        grouping(dimension_2) as dimension_2_bit,
        grouping(dimension_3) as dimension_3_bit,
        grouping(total_object) as total_bit,
        'sum(revenue_impact)' as metric_calculation,
        sum(revenue_impact) as metric_value,
    from
        {{ ref('churned_mrr') }}
    where
        timestamp between '2014-01-01' and current_date() + interval 365 day
  group by 
    grouping sets (
        (metric_month, dimension_1),
        (metric_month, dimension_2),
        (metric_month, dimension_3),
        (metric_month, total_object)
    )
)
select
    case
        when month_bit = 0 then 'month'
    end as date_grain,
    case
        when month_bit = 0 then metric_month
    end as metric_date,
    case
        when dimension_1_bit = 0 then dimension_1
        when dimension_2_bit = 0 then dimension_2
        when dimension_3_bit = 0 then dimension_3
        when total_bit = 0 then total_object
    end as slice_object,
    metric_value,
    metric_calculation
from
    cte_grouping_sets