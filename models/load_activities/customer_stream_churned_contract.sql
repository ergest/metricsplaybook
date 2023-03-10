{{
  config(materialized = 'table')
}}
select *
from read_csv_auto('raw_csvs/customer_stream_churned_contract.csv')
