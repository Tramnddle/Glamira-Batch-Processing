{{ config(materialized='view') }}

with o as (
  select
    *
    , lower(trim(email_address)) as email_norm
    , trim(current_url) as current_url_norm
    , trim(referrer_url) as referrer_url_norm
  from {{ ref('stg_order') }}
),

cust as (
  select
    user_db_id,
    lower(trim(email_address)) as cust_email_norm
  from {{ ref('stg_customer') }}
),

joined as (
  select
    o.*,
    cust.cust_email_norm,

    -- Email regex
    regexp_contains(o.email_norm, r'^[^@\s]+@[^@\s]+\.[^@\s]+$') as is_email_valid,

    -- IPv4 basic pattern
    regexp_contains(o.ip, r'^\d{1,3}(\.\d{1,3}){3}$') as is_ipv4_format,

    -- No placeholder IPs
    o.ip not in ('0.0.0.0','127.0.0.1') as is_not_placeholder_ip,

    -- No private ranges (IPv4)
    not (
      starts_with(o.ip, '10.')
      or regexp_contains(o.ip, r'^192\.168\.')
      or regexp_contains(o.ip, r'^172\.(1[6-9]|2[0-9]|3[0-1])\.')
    ) as is_not_private_ip,

    -- URL validity
    regexp_contains(o.current_url_norm, r'^https?://') as is_current_url_valid,
    regexp_contains(o.referrer_url_norm, r'^https?://') as is_referrer_url_valid,

    -- Optional: success URL for checkout_success
    case
      when o.collection = 'checkout_success' then regexp_contains(o.current_url_norm, r'/checkout/onepage/success')
      else true
    end as is_success_url_ok,

    -- Numeric rules
    o.product_quantity > 0 as is_qty_positive,
    o.product_price >= 0 as is_price_nonnegative,

    safe_cast(o.product_quantity as numeric) * safe_cast(o.product_price as numeric) as line_total_calc,
    (safe_cast(o.product_quantity as numeric) * safe_cast(o.product_price as numeric)) >= 0 as is_line_total_nonnegative,
    (safe_cast(o.product_quantity as numeric) * safe_cast(o.product_price as numeric)) <= 100000 as is_line_total_reasonable,

    -- Time rules (assumes time_stamp is epoch seconds; adjust if millis)
    timestamp_seconds(o.time_stamp) between timestamp('2015-01-01') and timestamp_add(current_timestamp(), interval 1 day)
      as is_event_ts_in_range,

    -- Cross-field consistency (email matches customer email for that user_db_id)
    case
      when o.user_db_id is null or o.email_norm is null then true
      when cust.cust_email_norm is null then true
      else o.email_norm = cust.cust_email_norm
    end as is_email_matches_customer

  from o
  left join cust
    using (user_db_id)
),

final as (
  select
    *,
    -- overall status
    (is_email_valid
      and is_ipv4_format
      and is_not_placeholder_ip
      and is_not_private_ip
      and is_current_url_valid
      and is_referrer_url_valid
      and is_success_url_ok
      and is_qty_positive
      and is_price_nonnegative
      and is_line_total_nonnegative
      and is_line_total_reasonable
      and is_event_ts_in_range
      and is_email_matches_customer
    ) as is_row_valid
  from joined
)

select * from final
