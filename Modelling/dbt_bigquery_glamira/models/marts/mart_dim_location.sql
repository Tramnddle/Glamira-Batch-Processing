{{ config(materialized='table') }}

select
  location_key,
  ip,
  country_code,
  country_name,
  region,
  city
from {{ ref('stg_location') }}
