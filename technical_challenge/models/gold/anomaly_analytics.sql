{{ config(materialized='table') }}

select 
    e.event_id,
    e.user_id,
    e.event_type,
    e.event_timestamp
from {{ ref('stage_event_logs') }} e
left join {{ ref('silver_logins') }} l 
    on e.user_id = l.user_id 
    and l.event_timestamp <= e.event_timestamp
where e.event_type != 'user_login'
  and l.event_id is null