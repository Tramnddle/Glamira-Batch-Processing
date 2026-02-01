{{ config(materialized='view') }}

select
  CAST(ABS(FARM_FINGERPRINT(
      CONCAT(
        COALESCE(LOWER(TRIM(CAST(country_code AS STRING))), ''),
        '|',
        COALESCE(LOWER(TRIM(CAST(region AS STRING))), ''),
        '|',
        COALESCE(LOWER(TRIM(CAST(city AS STRING))), '')
      )
    )) AS STRING) as location_key,

  -- remaining columns
  cast(ip as string) as ip,
  cast(country_code as string) as country_code,
  cast(country_name as string) as country_name,
  cast(region as string) as region,
  cast(city as string) as city,
  cast(isp as string) as isp

from {{ source('raw', 'ip_location_raw') }}
