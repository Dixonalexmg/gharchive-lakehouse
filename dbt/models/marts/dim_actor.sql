{{ config(materialized='table') }}

select
    actor_id,
    max(actor_login)             as actor_login,
    min(created_at)              as first_seen_at,
    max(created_at)              as last_seen_at,
    count(*)                     as total_events,
    count(distinct repo_id)      as unique_repos,
    count(distinct event_date)   as active_days,
    count(distinct event_type)   as distinct_event_types
from {{ ref('stg_events') }}
where actor_id is not null
group by actor_id
