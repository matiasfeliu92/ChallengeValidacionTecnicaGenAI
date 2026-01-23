{{ config(materialized='table') }}

with user_daily_session as (
    select
        user_id,
        date_trunc('day', event_timestamp)::date as session_date,
        min(event_timestamp) as first_event,
        max(event_timestamp) as last_event
    from {{ ref('stage_event_logs') }}
    group by 1, 2
)
select
    user_id,
    avg(last_event - first_event) as avg_session_duration
from user_daily_session
group by 1