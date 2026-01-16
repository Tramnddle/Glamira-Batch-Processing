{{ config(materialized='view') }}

with user_check as (
  select
    user_id_db,
    case
      when user_id_db is null then 'NULL'
      when trim(cast(user_id_db as string)) = '' then 'EMPTY'
      when lower(trim(cast(user_id_db as string))) in ('null','n/a','na','none','undefined') then 'FAKE_NULL'
      else 'VALID'
    end as user_status
  from {{ source('raw', 'countly_summary') }}
  group by user_id_db
),

checked_users as (
  select
    cs.email_address,
    uc.user_status,
    cs.user_id_db
  from user_check uc
  join {{ source('raw', 'countly_summary') }} cs
    on uc.user_id_db = cs.user_id_db
  where cs.email_address is not null
    and trim(cs.email_address) != ''
),

email_base as (
  select
    checked_users.email_address,
    case
      when count(distinct checked_users.user_id_db) = 1 then 'Unique'
      when count(distinct checked_users.user_id_db) > 1 then 'Multiple'
      else 'Unknown'
    end as email_category
  from checked_users
  where checked_users.user_status = 'VALID'
  group by checked_users.email_address
),

-- pick a representative user_db_id per email (needed because some emails can be "Multiple")
email_user as (
  select
    eb.email_address,
    eb.email_category,
    -- choose one deterministic user_id_db for the dimension row
    min(cast(cu.user_id_db as string)) as user_db_id
  from email_base eb
  join checked_users cu
    on cu.email_address = eb.email_address
   and cu.user_status = 'VALID'
  group by eb.email_address, eb.email_category
)

select
  -- deterministic “random-looking” bigint key based on email
  cast(
    abs(farm_fingerprint(lower(trim(email_address))))
    as int64
  ) as customer_key,

  cast(email_address as string) as email_address,
  cast(user_db_id as int64) as user_db_id,
  email_category

from email_user
order by email_address
