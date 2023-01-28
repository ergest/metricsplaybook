{{-
    config(materialized = 'table')
-}}

-- Returns a list of relations that match schema.prefix%
{%- set cube_tables = dbt_utils.get_relations_by_pattern('main', '%_cube') -%}
{{ dbt_utils.union_relations(relations = cube_tables) }}