{{ config(materialized='table') }}

with distinct_dates as (
    select distinct event_date as date_key
    from {{ ref('stg_events') }}
    where event_date is not null
)

select
    date_key,
    year(date_key)        as year,
    quarter(date_key)     as quarter,
    month(date_key)       as month,
    day(date_key)         as day,
    dayofweek(date_key)   as day_of_week,
    weekofyear(date_key)  as week_of_year,
    case when dayofweek(date_key) in (1, 7) then true else false end as is_weekend
from distinct_dates
