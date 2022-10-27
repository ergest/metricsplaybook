select *, substring(date_trunc('month', dc.first_contract_signed_date)::text, 1, 7) as cohort
from dim_customer