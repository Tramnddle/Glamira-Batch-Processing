{{ config(materialized='view') }}

WITH
  base AS (
    SELECT
      CAST(order_id AS string) AS order_id,
      CAST(time_stamp AS int64) AS time_stamp,
      timestamp_seconds(CAST(time_stamp AS int64)) AS event_ts,
      CAST(local_time AS string) AS local_time,
      CAST(collection AS string) AS collection,
      CAST(ip AS string) AS ip,
      CAST(user_agent AS string) AS user_agent,
      CAST(resolution AS string) AS resolution,

      -- rename user_id_db -> user_db_id
      SAFE_CAST(user_id_db AS int64) AS user_db_id,
      CAST(device_id AS string) AS device_id,
      CAST(api_version AS string) AS api_version,
      CAST(store_id AS string) AS store_id,
      CAST(show_recommendation AS string) AS show_recommendation,
      CAST(current_url AS string) AS current_url,
      CAST(referrer_url AS string) AS referrer_url,
      CAST(email_address AS string) AS email_address,
      cart_products
    FROM `solid-transport-479213-h5`.`raw`.`countly_summary`
    WHERE collection = 'checkout_success'
  ),

  line_items AS (
    SELECT
      b.*,

      -- cart_products fields
      SAFE_CAST(cp.product_id AS int64) AS product_id,
      SAFE_CAST(cp.amount AS int64) AS product_quantity,

      SAFE.PARSE_NUMERIC(
        TRIM(
          REPLACE(
            CASE
              WHEN
                STRPOS(REPLACE(cp.price, '٫', '.'), ',') > 0
                AND STRPOS(REPLACE(cp.price, '٫', '.'), '.') > 0
              THEN
                IF(
                  STRPOS(REPLACE(cp.price, '٫', '.'), ',')
                    < STRPOS(REPLACE(cp.price, '٫', '.'), '.'),
                  REPLACE(REPLACE(cp.price, '٫', '.'), ',', ''),
                  REPLACE(
                    REPLACE(REPLACE(cp.price, '٫', '.'), '.', ''), ',', '.'
                  )
                )
              WHEN STRPOS(REPLACE(cp.price, '٫', '.'), ',') > 0
              THEN REPLACE(REPLACE(cp.price, '٫', '.'), ',', '.')
              ELSE REPLACE(cp.price, '٫', '.')
            END,
            ' ',
            ''
          )
        )
      ) AS product_price,

      CAST(cp.currency AS string) AS product_currency,
      cp.option AS option_array,
      cp_offset
    FROM base b
    LEFT JOIN UNNEST(b.cart_products) AS cp WITH OFFSET AS cp_offset
  ),

  options AS (
    SELECT
      li.*,
      CAST(opt.option_id AS string) AS option_id,
      CAST(opt.option_label AS string) AS option_label,
      opt_offset
    FROM line_items li
    LEFT JOIN UNNEST(li.option_array) AS opt WITH OFFSET AS opt_offset
  ),

  locations AS (
    SELECT
      CAST(ip AS string) AS ip,
      location_key
    FROM {{ ref('stg_location') }}
  )

SELECT
  TO_HEX(
    MD5(
      CONCAT(
        COALESCE(order_id, ''),
        '|',
        COALESCE(CAST(product_id AS string), ''),
        '|',
        COALESCE(option_id, ''),
        '|',
        CAST(COALESCE(cp_offset, -1) AS string),
        '|',
        CAST(COALESCE(opt_offset, -1) AS string),
        '|',
        CAST(COALESCE(time_stamp, -1) AS string)
      )
    )
  ) AS item_key,

  -- ✅ bring in location_key from stg_location
  l.location_key,

  order_id,
  time_stamp,
  event_ts,
  local_time,
  collection,
  o.ip,
  user_agent,
  resolution,
  user_db_id,
  device_id,
  api_version,
  store_id,
  show_recommendation,
  current_url,
  referrer_url,
  email_address,

  product_id,
  product_quantity,
  product_price,
  product_currency,
  option_id,
  option_label,

  CAST(product_quantity AS numeric) * CAST(product_price AS numeric) AS line_total_amount
FROM options o
LEFT JOIN locations l
  ON o.ip = l.ip
