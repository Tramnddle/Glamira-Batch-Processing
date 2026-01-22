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
    cu.email_address,
    case
      when count(distinct cast(cu.user_id_db as string)) = 1 then 'Unique'
      when count(distinct cast(cu.user_id_db as string)) > 1 then 'Multiple'
      else 'Unknown'
    end as has_email_multiple_user_ids
  from checked_users cu
  where cu.user_status = 'VALID'
  group by cu.email_address
),

email_user_pairs as (
  select distinct
    cu.email_address,
    cast(cu.user_id_db as string) as user_db_id
  from checked_users cu
  where cu.user_status = 'VALID'
)

select
  eup.email_address,
  safe_cast(eup.user_db_id as int64) as user_db_id,
  eb.has_email_multiple_user_ids
from email_user_pairs eup
join email_base eb
  on eb.email_address = eup.email_address
order by eup.email_address, user_db_id
