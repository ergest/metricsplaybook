{{-
    config(materialized = 'table')
-}}

{{-
generate_fake_data (
    activity_name = 'committed_to_churn',
    has_revenue_impact = true,
    feature_json_dict = '{"segment": ["seg1", "seg2", "seg3", "seg4"],
                         "plan": ["plan1", "plan2", "plan3"]}'
  )
-}}
