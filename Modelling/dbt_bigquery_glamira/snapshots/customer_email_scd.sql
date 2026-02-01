{% snapshot customer_email_scd %}

{# One snapshot record per user_db_id + email #}
{# Use per-event unique_key so every event is kept (SCD Type 2) #}
{{
  config(
    target_schema='snapshots',
    unique_key="concat(cast(user_db_id as string), '|', coalesce(lower(trim(email_address)), ''), '|', cast(time_stamp as string))",
    strategy='timestamp',
    updated_at='event_ts'
  )
}}

with fact as (
  select
    cast(o.user_db_id as int64) as user_db_id,
    lower(trim(cast(o.email_address as string))) as email_address,

    cast(o.time_stamp as int64) as time_stamp,

    -- convert epoch seconds into timestamp/datetime/date
    timestamp_seconds(cast(o.time_stamp as int64)) as event_ts,
    date(timestamp_seconds(cast(o.time_stamp as int64))) as date,
    datetime(timestamp_seconds(cast(o.time_stamp as int64))) as time

  from {{ ref('stg_order') }} as o
  join {{ ref('stg_customer') }} as c
    on cast(o.user_db_id as int64) = c.user_db_id
  where o.user_db_id is not null
    and o.email_address is not null
    and trim(o.email_address) != ''
),

-- For SCD2: determine latest time_stamp per user to mark current vs historical
-- Rank events per user and mark the most recent as current
ranked as (
  select
    f.*,
    row_number() over (partition by f.user_db_id order by f.event_ts desc) as rn
  from fact as f
)

select
  r.user_db_id,
  r.email_address,
  r.time_stamp,
  r.event_ts,
  r.date,
  r.time,
  r.rn
from ranked as r

{% endsnapshot %}
