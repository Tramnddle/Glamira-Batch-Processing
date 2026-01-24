{{ config(materialized='table') }}

with stg as (
  select
    cast(user_db_id as int64) as user_db_id,
    lower(trim(cast(email_address as string))) as email_address,
    cast(has_email_multiple_user_ids as string) as has_email_multiple_user_ids
  from {{ ref('stg_customer') }}
  where user_db_id is not null
),

-- 1) classify each user_db_id as unique vs multiple emails
user_email_stats as (
  select
    user_db_id,
    count(distinct email_address) as distinct_emails,
    case
      when count(distinct email_address) > 1 then 'multiple'
      else 'unique'
    end as has_user_db_id_multiple_email_addresses
  from stg
  where email_address is not null and email_address != ''
  group by user_db_id
),

-- 2) temp view: only users having multiple emails
users_multiple as (
  select user_db_id
  from user_email_stats
  where has_user_db_id_multiple_email_addresses = 'multiple'
),

-- latest email for those users from mart_fact_order, based on time_stamp
latest_email_from_fact as (
  select
    user_db_id,
    lower(trim(email_address)) as latest_email_address,
    time_stamp,
    row_number() over (
      partition by user_db_id
      order by cast(time_stamp as int64) desc, lower(trim(email_address)) desc
    ) as rn
  from {{ ref('stg_order') }}
  where user_db_id is not null
    and email_address is not null
    and trim(email_address) != ''
),

latest_email_per_user as (
  select
    user_db_id,
    latest_email_address
  from latest_email_from_fact
  where rn = 1
),

-- 3) join back and replace email if needed
resolved as (
  select
    s.user_db_id,
    s.email_address as original_email_address,
    s.has_email_multiple_user_ids,

    ues.has_user_db_id_multiple_email_addresses,

    le.latest_email_address,

    case
      when ues.has_user_db_id_multiple_email_addresses = 'multiple'
        then coalesce(le.latest_email_address, s.email_address)
      else s.email_address
    end as final_email_address
  from stg s
  left join user_email_stats ues
    on ues.user_db_id = s.user_db_id
  left join latest_email_per_user le
    on le.user_db_id = s.user_db_id
),

-- ensure 1 row per user_db_id (dimension grain)
dedup as (
  select
    user_db_id,
    -- for unique users: final_email_address is already stable
    -- for multiple: final_email_address is latest email from fact
    any_value(final_email_address) as email_address,
    any_value(has_email_multiple_user_ids) as has_email_multiple_user_ids,
    any_value(has_user_db_id_multiple_email_addresses) as has_user_db_id_multiple_email_addresses,
    any_value(original_email_address) as original_email_address,
    any_value(latest_email_address) as latest_email_address
  from resolved
  group by user_db_id
)

select
  -- 4) customer_key (primary key) from final email address
  cast(
    abs(farm_fingerprint(coalesce(lower(trim(email_address)), '')))
    as int64
  ) as customer_key,

  user_db_id,
  email_address,
  has_user_db_id_multiple_email_addresses,

  -- optional debug fields (keep for QA; remove later if you want)
  has_email_multiple_user_ids,
  original_email_address,
  latest_email_address

from dedup
