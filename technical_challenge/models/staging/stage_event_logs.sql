{{ config(materialized='view') }}

with source as (
    select * from {{ source('raw', 'raw_event_logs') }}
),

flattened as (
    select
        (data ->> 'event_id')::uuid as event_id,
        (data ->> 'timestamp')::timestamp as event_timestamp,
        (data ->> 'event_type') as event_type,
        (data ->> 'user_id') as user_id,
        (data ->> 'document_id') as document_id,
        data as raw_data
    from source
)

select * from flattened