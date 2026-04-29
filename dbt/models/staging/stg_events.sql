{{ config(materialized='view') }}

select
    event_id,
    event_type,
    actor_id,
    actor_login,
    repo_id,
    repo_name,
    payload_json,
    public,
    created_at,
    event_date,
    event_hour
from delta.`{{ var('silver_path') }}`
