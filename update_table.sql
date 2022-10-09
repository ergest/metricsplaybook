create or replace table contract_stream as
select * 
from read_csv_auto('/Users/ergestxheblati/Downloads/contract_stream.csv');

create or replace table dim_customer as
select * 
from read_csv_auto('/Users/ergestxheblati/Downloads/dim_customer.csv');

update contract_stream a
set activity_occurrence = dt.activity_occurrence,
    activity_repeated_at = dt.activity_repeated_at
from (
    select
        id,
        customer_id,
        activity,
        timestamp,
        row_number() over(partition by customer_id, activity order by timestamp asc) as activity_occurrence,
        lead(timestamp, 1) over(partition by customer_id, activity order by timestamp asc) as activity_repeated_at
    from 
        contract_stream
)dt
where 
    dt.id = a.id
    and dt.customer_id = a.customer_id
    and dt.activity = a.activity
    and a.timestamp = dt.timestamp;