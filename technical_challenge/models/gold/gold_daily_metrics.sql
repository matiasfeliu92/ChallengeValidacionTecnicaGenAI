{{ config(materialized='table') }}

select
    date_trunc('day', event_timestamp) as report_date,
    count(distinct user_id) as daily_active_users,
    count(distinct case when event_type = 'user_login' then event_id end) as total_logins,
    count(distinct case when event_type = 'document_edit' then event_id end) as total_edits
from {{ ref('stage_event_logs') }}
group by 1