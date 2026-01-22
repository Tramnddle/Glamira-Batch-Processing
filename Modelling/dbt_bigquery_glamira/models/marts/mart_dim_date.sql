{{ config(materialized='table') }}

with dates as (
  select d as date
  from unnest(generate_date_array(date '2015-01-01', date '2050-12-31')) as d
)

select
  date,
  extract(year from date) as year,
  extract(month from date) as month
from dates
order by date
