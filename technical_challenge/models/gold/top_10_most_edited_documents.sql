{{ config(materialized='table') }}

select
    document_id,
    sum(edit_length) as total_edit_volume,
    count(event_id) as total_edit_events
from {{ ref('silver_edits') }}
group by 1
order by total_edit_volume desc
limit 10