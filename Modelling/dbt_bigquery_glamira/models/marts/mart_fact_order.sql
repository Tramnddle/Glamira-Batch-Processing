{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_order') }}
),

/* 1) Clean + map raw currency values to ISO codes */
currency_clean as (
  select
    s.*,

    -- normalize the raw currency text for matching
    trim(s.product_currency) as product_currency_raw,
    lower(trim(s.product_currency)) as product_currency_norm,

    case
      -- Empty / missing
      when s.product_currency is null or trim(s.product_currency) = '' then null

      -- High-confidence explicit labels
      when lower(trim(s.product_currency)) in ('usd $', 'usd', 'us$', 'us $') then 'USD'
      when lower(trim(s.product_currency)) = 'cad $' then 'CAD'
      when lower(trim(s.product_currency)) = 'au $'  then 'AUD'
      when lower(trim(s.product_currency)) = 'sgd $' then 'SGD'
      when lower(trim(s.product_currency)) = 'nzd $' then 'NZD'
      when lower(trim(s.product_currency)) = 'hkd $' then 'HKD'
      when lower(trim(s.product_currency)) = 'mxn $' then 'MXN'
      when lower(trim(s.product_currency)) = 'cop $' then 'COP'
      when lower(trim(s.product_currency)) = 'dop $' then 'DOP'
      when lower(trim(s.product_currency)) = 'gtq q' then 'GTQ'
      when lower(trim(s.product_currency)) in ('pen s/.', 's/.', 's/. ') then 'PEN'
      when lower(trim(s.product_currency)) = 'clp' then 'CLP'
      when lower(trim(s.product_currency)) = 'uyu' then 'UYU'
      when lower(trim(s.product_currency)) = 'chf' then 'CHF'
      when lower(trim(s.product_currency)) = 'lei' then 'RON'
      when lower(trim(s.product_currency)) = 'ft' then 'HUF'
      when lower(trim(s.product_currency)) = 'kč' then 'CZK'
      when lower(trim(s.product_currency)) = 'zł' then 'PLN'
      when lower(trim(s.product_currency)) in ('лв.', 'лв') then 'BGN'
      when lower(trim(s.product_currency)) = 'kn' then 'HRK'   -- legacy currency (Croatia pre-EUR)
      when lower(trim(s.product_currency)) in ('din.', ' din.', 'din') then 'RSD'
      when lower(trim(s.product_currency)) = 'r$' then 'BRL'

      -- Symbols (some are safe, some ambiguous)
      when trim(s.product_currency) = '€' then 'EUR'
      when trim(s.product_currency) = '£' then 'GBP'
      when trim(s.product_currency) = '₺' then 'TRY'
      when trim(s.product_currency) = '₫' then 'VND'
      when trim(s.product_currency) = '₱' then 'PHP'
      when trim(s.product_currency) = '₹' then 'INR'
      when trim(s.product_currency) = '￥' then 'JPY'
      when trim(s.product_currency) = '₲' then 'PYG'
      when trim(s.product_currency) = 'د.ك.‏' then 'KWD'

      -- Ambiguous symbol-only values
      when trim(s.product_currency) = '$' then null          -- needs context (US/CA/AU/SG/NZ/etc.)
      when lower(trim(s.product_currency)) = 'kr' then null  -- needs context (SEK/NOK/DKK)

      else null
    end as currency_code,

    case
      when s.product_currency is null or trim(s.product_currency) = '' then 'UNKNOWN'

      when lower(trim(s.product_currency)) in (
        'usd $','usd','us$','us $','cad $','au $','sgd $','nzd $','hkd $','mxn $','cop $','dop $',
        'gtq q','pen s/.','clp','uyu','chf','lei','ft','kč','zł','лв.','лв','kn','din.',' din.','din','r$'
      ) then 'CLEAN'

      when trim(s.product_currency) in ('€','£','₺','₫','₱','₹','￥','₲','د.ك.‏') then 'CLEAN'

      when trim(s.product_currency) in ('$') then 'AMBIGUOUS'
      when lower(trim(s.product_currency)) in ('kr') then 'AMBIGUOUS'

      else 'UNKNOWN'
    end as currency_status

  from src s
),

fx_latest as (
  select
    fx_date,
    base_code,
    currency_code,
    cast(usd_to_ccy as numeric) as usd_to_ccy
  from {{ ref('fx_to_usd_22_01_26') }}
  where fx_date = (select max(fx_date) from {{ ref('fx_to_usd_22_01_26') }})
),

/* 3) Join rates + compute USD amounts
      Seed table provides: 1 USD = usd_to_ccy (in that currency)
      so: amount_usd = amount_ccy / usd_to_ccy */
final as (
  select
    -- keep your staging PK
    c.item_key,

    -- keys / ids
    c.order_id,
    c.product_id,
    c.option_id,
    c.user_db_id,
    c.ip,

    -- raw timestamp
    c.time_stamp,

    -- derived time fields
    timestamp_seconds(cast(c.time_stamp as int64)) as event_ts,
    datetime(timestamp_seconds(cast(c.time_stamp as int64))) as event_datetime,

    date(timestamp_seconds(cast(c.time_stamp as int64))) as date,
    datetime(timestamp_seconds(cast(c.time_stamp as int64))) as time,

    -- attributes
    c.collection,
    c.product_currency_raw as product_currency,
    c.currency_code,
    c.currency_status,
    c.email_address,
    c.device_id,
    c.user_agent,
    c.resolution,
    c.store_id,
    c.local_time,
    c.current_url,
    c.referrer_url,
    c.show_recommendation,
    c.location_key,
    -- measures (original)
    c.product_quantity,
    c.product_price,
    c.line_total_amount,

    -- USD conversions (FX columns removed from the fact output)
    case
      when c.currency_code is null then null
      when fx.usd_to_ccy is null then null
      else safe_cast(c.product_price as numeric) / fx.usd_to_ccy
    end as product_price_usd,

    case
      when c.currency_code is null then null
      when fx.usd_to_ccy is null then null
      else safe_cast(c.line_total_amount as numeric) / fx.usd_to_ccy
    end as line_total_amount_usd

  from currency_clean c
  left join fx_latest fx
    on fx.currency_code = c.currency_code
),

final_cust as (
  select 
    fl.* except(email_address),
    cust.customer_key,
    cust.email_address
  from final fl
left join {{ ref('mart_dim_customer') }} as cust
    on fl.email_address = cust.email_address
)

select 
  fc.* except(product_id),
  prod.product_key,
  prod.product_id
from final_cust fc
left join {{ ref('mart_dim_product') }} as prod
  on fc.product_id = prod.product_id
