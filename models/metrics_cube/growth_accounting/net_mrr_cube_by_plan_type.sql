{{-
    config(materialized = 'table')
-}}

{{ generate_net_mrr_cube('plan_type') }}