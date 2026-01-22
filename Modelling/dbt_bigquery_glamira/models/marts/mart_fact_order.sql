{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_order') }}
)

select
  -- keep your staging PK
  item_key,

  -- keys / ids
  order_id,
  product_id,
  option_id,
  user_db_id,
  ip,

  -- raw timestamp
  time_stamp,

  -- derived time fields
  timestamp_seconds(cast(time_stamp as int64)) as event_ts,
  datetime(timestamp_seconds(cast(time_stamp as int64))) as event_datetime,

  date(timestamp_seconds(cast(time_stamp as int64))) as date,
  datetime(timestamp_seconds(cast(time_stamp as int64))) as time,

  -- attributes
  collection,
  product_currency,
  email_address,
  device_id,
  user_agent,
  resolution,
  store_id,
  local_time,
  current_url,
  referrer_url,
  show_recommendation,

  -- measures
  product_quantity,
  product_price,
  line_total_amount

from src

