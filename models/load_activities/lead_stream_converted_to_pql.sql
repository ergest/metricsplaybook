{{
  config(materialized = 'table')
}}
select *
from read_csv_auto('raw_csvs/lead_stream_converted_to_pql.csv')
