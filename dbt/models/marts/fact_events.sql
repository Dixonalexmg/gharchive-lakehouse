{{ config(
    materialized='table',
    partition_by=['event_date']
) }}

select
    event_id,
    event_type,
    repo_id,
    actor_id,
    event_date,
    event_hour,
    created_at,
    public
from {{ ref('stg_events') }}
