{{ config(materialized='view') }}

with c as (
  select
    customer_key,
    lower(trim(email_address)) as email_norm,
    user_db_id
  from {{ ref('stg_customer') }}
  where email_address is not null and trim(email_address) != ''
),

agg as (
  select
    email_norm,
    count(distinct user_db_id) as distinct_user_db_ids
  from c
  group by email_norm
)

select
  c.customer_key,
  c.email_norm as email_address,
  c.user_db_id,

  regexp_contains(c.email_norm, r'^[^@\s]+@[^@\s]+\.[^@\s]+$') as is_email_valid,
  (a.distinct_user_db_ids = 1) as is_email_unique_to_user,
  (a.distinct_user_db_ids > 1) as has_email_multiple_users

from c
join agg a using (email_norm)
