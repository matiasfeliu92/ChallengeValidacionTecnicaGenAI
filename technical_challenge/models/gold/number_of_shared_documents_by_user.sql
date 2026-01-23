{{ config(materialized='table') }}

select
    user_id,
    count(event_id) as total_shares_initiated
from {{ ref('silver_shares') }}
group by 1
order by total_shares_initiated desc