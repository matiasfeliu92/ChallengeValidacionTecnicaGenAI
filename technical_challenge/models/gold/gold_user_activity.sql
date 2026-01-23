{{ config(materialized='table') }}

with logins as (
    select user_id, count(event_id) as total_logins
    from {{ ref('silver_logins') }}
    group by 1
),
comments as (
    select user_id, count(event_id) as total_comments
    from {{ ref('silver_comments') }}
    group by 1
),
edits as (
    select user_id, sum(edit_length) as total_chars_edited, count(event_id) as total_edits
    from {{ ref('silver_edits') }}
    group by 1
)

select
    u.user_id,
    coalesce(l.total_logins, 0) as logins,
    coalesce(c.total_comments, 0) as comments,
    coalesce(e.total_edits, 0) as edits,
    coalesce(e.total_chars_edited, 0) as volume_edited
from (select distinct user_id from {{ ref('stage_event_logs') }}) u
left join logins l on u.user_id = l.user_id
left join comments c on u.user_id = c.user_id
left join edits e on u.user_id = e.user_id