{{ config(materialized='table') }}

select
    document_id,
    count(distinct user_id) as unique_users,
    count(case when event_type = 'comment_added' then 1 end) as total_comments,
    count(case when event_type = 'document_shared' then 1 end) as times_shared,
    sum(case when event_type = 'document_edit' then (raw_data ->> 'edit_length')::integer else 0 end) as total_edit_volume
from {{ ref('stage_event_logs') }}
group by 1