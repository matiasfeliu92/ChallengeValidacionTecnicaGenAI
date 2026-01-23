{{ config(materialized='table') }}

select
    date_trunc('day', event_timestamp)::date as event_date,
    count(distinct user_id) as daily_active_users
from {{ ref('stage_event_logs') }}
where event_timestamp >= current_date - interval '30 days'
group by 1
order by 1 desc