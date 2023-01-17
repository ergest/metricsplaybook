{%-
    macro generate_total_mrr_cube (
        dimension,
        is_snapshot_reliant_metric = false
    )
-%}

select
    '{{ model.name }}' as metric_model,
    date_grain,
    metric_date,
    slice_dimension,
    slice_value,
    'cumulative sum of net_revenue' as metric_calculation,
    sum(metric_value) over(partition by slice_value order by metric_date) as metric_value
from
    {{ ref('net_mrr_cube_by_'+dimension) }}
where
    slice_dimension = '{{dimension}}'

{%- endmacro %}