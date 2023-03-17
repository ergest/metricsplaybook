{{-
    config(materialized = 'table')
-}}

{{-
generate_fake_data (
    activity_name = 'churned_contract',
    has_revenue_impact = true,
    feature_json_dict = '{"subcsription_type": ["mrr", "arr"],
                         "segment": ["seg1", "seg2", "seg3", "seg4"],
                         "plan": ["plan1", "plan2", "plan3"],
                         "csm": ["csm1", "csm2", "csm3", "csm4", "csm5"],
                         "mrr_tier": ["tier1", "tier2"]}'
  )
-}}
