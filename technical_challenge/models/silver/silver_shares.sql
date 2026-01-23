{{ config(
    materialized='incremental',
    unique_key='event_id',
    incremental_strategy='merge'
) }}

with stage_events as (
    select * from {{ ref('stage_event_logs') }}
    where event_type = 'document_shared'
)

select
    event_id,
    event_timestamp,
    user_id,
    document_id,
    (raw_data ->> 'shared_with') as shared_with
from stage_events

{% if is_incremental() %}
  where event_timestamp > (select max(event_timestamp) from {{ this }})
{% endif %}