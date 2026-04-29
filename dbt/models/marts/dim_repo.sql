{{ config(materialized='table') }}

select
    repo_id,
    max(repo_name)                                                       as repo_name,
    min(created_at)                                                      as first_seen_at,
    max(created_at)                                                      as last_seen_at,
    count(*)                                                             as total_events,
    count(distinct actor_id)                                             as unique_actors,
    count(distinct event_date)                                           as active_days,
    sum(case when event_type = 'PushEvent'         then 1 else 0 end)    as push_events,
    sum(case when event_type = 'PullRequestEvent'  then 1 else 0 end)    as pr_events,
    sum(case when event_type = 'IssuesEvent'       then 1 else 0 end)    as issue_events,
    sum(case when event_type = 'WatchEvent'        then 1 else 0 end)    as watch_events,
    sum(case when event_type = 'ForkEvent'         then 1 else 0 end)    as fork_events
from {{ ref('stg_events') }}
where repo_id is not null
group by repo_id
