{{
    config(materialized = 'table')
}}

select * 
from read_csv_auto('seeds/contract_stream.csv')