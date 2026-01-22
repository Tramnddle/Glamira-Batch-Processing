{{ config(materialized='table') }}

with base as (
  select
    current_timestamp() as run_ts,
    count(*) as rows_total,

    sum(case when order_id is null then 1 else 0 end) as null_order_id,
    sum(case when event_ts is null then 1 else 0 end) as null_event_ts,
    sum(case when user_db_id is null then 1 else 0 end) as null_user_db_id,
    sum(case when product_id is null then 1 else 0 end) as null_product_id,
    sum(case when product_quantity is null then 1 else 0 end) as null_product_quantity,
    sum(case when product_price is null then 1 else 0 end) as null_price,
    sum(case when product_currency is null then 1 else 0 end) as null_currency,
    sum(case when ip is null then 1 else 0 end) as null_ip
  from {{ ref('stg_order') }}
)

select
  run_ts,
  rows_total,
  safe_divide(null_order_id, rows_total) as null_rate_order_id,
  safe_divide(null_event_ts, rows_total) as null_rate_event_ts,
  safe_divide(null_user_db_id, rows_total) as null_rate_user_db_id,
  safe_divide(null_product_id, rows_total) as null_rate_product_id,
  safe_divide(null_product_quantity, rows_total) as null_rate_product_quantity,
  safe_divide(null_price, rows_total) as null_rate_price,
  safe_divide(null_currency, rows_total) as null_rate_currency,
  safe_divide(null_ip, rows_total) as null_rate_ip
from base

