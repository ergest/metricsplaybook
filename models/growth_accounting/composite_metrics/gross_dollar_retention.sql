{{-
    config(materialized = 'table')
-}}

select
    '{{ model.name }}' as metric_model,
    date_grain,
    metric_date,
    slice_dimension,
    slice_value,
    '1 - gross_revenue_churn_rate' as metric_calculation,
    1 - metric_value as metric_value
from
    {{ ref('gross_revenue_churn_rate_cube') }}