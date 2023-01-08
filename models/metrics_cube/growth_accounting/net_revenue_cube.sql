{{-
    config(materialized = 'table')
-}}

with cte_grouping_sets as (
    select
        month as metric_month,
        grouping(metric_month) as month_bit,
        'Total' as total_object,
        grouping(total_object) as total_bit,
        'sum(net_recurring_revenue)' as metric_calculation,
        sum(net_recurring_revenue) as metric_value,
    from
        {{ ref('net_revenue') }}
    where
        month between '2014-01-01' and current_date() + interval 365 day
  group by 
    grouping sets (
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
        when total_bit = 0 then total_object
    end as slice_object,
    metric_value,
    metric_calculation
from
    cte_grouping_sets