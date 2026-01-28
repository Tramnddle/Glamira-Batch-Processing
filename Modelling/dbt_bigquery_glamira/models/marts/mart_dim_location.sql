{{ config(materialized='table') }}

with ranked as (
  select
    location_key,
    ip,
    country_code,
    country_name,
    region,
    city,
    row_number() over (
      partition by location_key
      order by ip
    ) as rn
  from {{ ref('stg_location') }}
)

select
  location_key,
  ip,
  country_code,
  country_name,
  region,
  city
from ranked
where rn = 1

