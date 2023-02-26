{{
    config(materialized = 'table')
}}

select *
from read_csv_auto('raw_csvs/subscription.csv')