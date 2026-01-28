{{ config(materialized='view') }}

select
  to_hex(
    md5(
      concat(
        coalesce(lower(trim(cast(country_code as string))), ''),
        '|',
        coalesce(lower(trim(cast(region as string))), ''),
        '|',
        coalesce(lower(trim(cast(city as string))), '')
      )
    )
  ) as location_key,

  -- remaining columns
  cast(ip as string) as ip,
  cast(country_code as string) as country_code,
  cast(country_name as string) as country_name,
  cast(region as string) as region,
  cast(city as string) as city,
  cast(isp as string) as isp

from {{ source('raw', 'ip_location_raw') }}
