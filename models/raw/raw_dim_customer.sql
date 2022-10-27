{{
    config(materialized = 'table')
}}

select *, substring(date_trunc('month', first_contract_signed_date)::text, 1, 7) as cohort
from read_csv_auto('seeds/dim_customer.csv')