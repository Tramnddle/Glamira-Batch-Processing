{{ config(materialized='table') }}

with base as (
  select
    current_timestamp() as run_ts,
    'stg_order' as fact_table,

    -- orphan checks
    count(*) as fact_rows,

    sum(case when p.product_id is null then 1 else 0 end) as orphan_product_rows,
    sum(case when c.user_db_id is null then 1 else 0 end) as orphan_customer_rows,
    sum(case when l.ip is null then 1 else 0 end) as orphan_location_rows

  from {{ ref('stg_order') }} o
  left join {{ ref('stg_product') }}  p on o.product_id = p.product_id
  left join {{ ref('stg_customer') }} c on o.user_db_id = c.user_db_id
  left join {{ ref('stg_location') }} l on o.ip = l.ip
)

select
  run_ts,
  fact_table,
  fact_rows,
  orphan_product_rows,
  safe_divide(orphan_product_rows, fact_rows) as orphan_product_rate,
  orphan_customer_rows,
  safe_divide(orphan_customer_rows, fact_rows) as orphan_customer_rate,
  orphan_location_rows,
  safe_divide(orphan_location_rows, fact_rows) as orphan_location_rate
from base

