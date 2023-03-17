{{
    config(
        materialized = 'table',
        post_hook = "update {{ this }} a
                    set activity_occurrence = dt.activity_occurrence,
                        activity_repeated_at = dt.activity_repeated_at
                    from (
                        select
                            id,
                            customer_id,
                            activity,
                            plan,
                            timestamp,
                            row_number() over(partition by customer_id, activity order by timestamp asc) as activity_occurrence,
                            lead(timestamp, 1) over(partition by customer_id, activity order by timestamp asc) as activity_repeated_at
                        from 
                            {{ this }}
                    )dt
                    where 
                        dt.id = a.id
                        and dt.customer_id = a.customer_id
                        and dt.activity = a.activity
                        and a.timestamp = dt.timestamp"
    )
}}
select 1