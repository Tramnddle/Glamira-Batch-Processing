INSERT INTO `solid-transport-479213-h5.raw.countly_summary_test` (
  api_version,
  cart_products,
  cat_id,
  collect_id,
  collection,
  currency,
  current_url,
  device_id,
  email_address,
  ip,
  is_paypal,
  key_search,
  local_time,
  option,
  order_id,
  price,
  product_id,
  recommendation,
  recommendation_clicked_position,
  recommendation_product_id,
  recommendation_product_position,
  referrer_url,
  resolution,
  show_recommendation,
  store_id,
  time_stamp,
  user_agent,
  user_id_db,
  utm_medium,
  utm_source,
  viewing_product_id
)
SELECT
  s.api_version,

  -- cart_products: JSON string -> ARRAY<STRUCT>
  ARRAY(
    SELECT AS STRUCT
      JSON_VALUE(cp, '$.product_id')                   AS product_id,
      SAFE_CAST(JSON_VALUE(cp, '$.amount') AS NUMERIC) AS amount,
      SAFE_CAST(JSON_VALUE(cp, '$.price')  AS NUMERIC) AS price,
      JSON_VALUE(cp, '$.currency')                     AS currency,

      ARRAY(
        SELECT AS STRUCT
          JSON_VALUE(op, '$.option_label') AS option_label,
          JSON_VALUE(op, '$.option_id')    AS option_id,
          JSON_VALUE(op, '$.value_label')  AS value_label,
          JSON_VALUE(op, '$.value_id')     AS value_id
        FROM UNNEST(IFNULL(JSON_QUERY_ARRAY(cp, '$.option'), [])) AS op
      ) AS option
    FROM UNNEST(IFNULL(JSON_QUERY_ARRAY(s.cart_products), [])) AS cp
  ) AS cart_products,

  s.cat_id,
  s.collect_id,
  s.collection,
  s.currency,
  s.current_url,
  s.device_id,
  s.email_address,
  s.ip,

  -- is_paypal STRING -> BOOLEAN
  CASE
    WHEN LOWER(s.is_paypal) IN ('true','1','yes','y') THEN TRUE
    WHEN LOWER(s.is_paypal) IN ('false','0','no','n') THEN FALSE
    ELSE NULL
  END AS is_paypal,

  s.key_search,

  -- local_time STRING -> DATETIME (if format matches)
  SAFE_CAST(s.local_time AS DATETIME) AS local_time,

  -- option JSON string -> STRUCT (ALL subfields STRING, incl price)
  STRUCT<
    kollektion STRING,
    alloy STRING,
    category_id STRING,
    diamond STRING,
    finish STRING,
    kollektion_id STRING,
    option_id STRING,
    option_label STRING,
    pearlcolor STRING,
    price STRING,
    quality STRING,
    quality_label STRING,
    shapediamond STRING,
    stone STRING,
    value_id STRING,
    value_label STRING
  >(
    JSON_VALUE(s.option, '$.kollektion'),
    JSON_VALUE(s.option, '$.alloy'),
    JSON_VALUE(s.option, '$.category_id'),
    JSON_VALUE(s.option, '$.diamond'),
    JSON_VALUE(s.option, '$.finish'),
    JSON_VALUE(s.option, '$.kollektion_id'),
    JSON_VALUE(s.option, '$.option_id'),
    JSON_VALUE(s.option, '$.option_label'),
    JSON_VALUE(s.option, '$.pearlcolor'),
    JSON_VALUE(s.option, '$.price'),         -- IMPORTANT: STRING (matches destination)
    JSON_VALUE(s.option, '$.quality'),
    JSON_VALUE(s.option, '$.quality_label'),
    JSON_VALUE(s.option, '$.shapediamond'),
    JSON_VALUE(s.option, '$.stone'),
    JSON_VALUE(s.option, '$.value_id'),
    JSON_VALUE(s.option, '$.value_label')
  ) AS option,

  s.order_id,

  -- top-level price STRING -> NUMERIC (destination column is NUMERIC)
  SAFE_CAST(s.price AS NUMERIC) AS price,

  s.product_id,
  s.recommendation,

  SAFE_CAST(s.recommendation_clicked_position AS INT64) AS recommendation_clicked_position,

  s.recommendation_product_id,

  NULL AS recommendation_product_position,

  s.referrer_url,
  s.resolution,

  -- show_recommendation STRING -> BOOLEAN
  CASE
    WHEN LOWER(s.show_recommendation) IN ('true','1','yes','y') THEN TRUE
    WHEN LOWER(s.show_recommendation) IN ('false','0','no','n') THEN FALSE
    ELSE NULL
  END AS show_recommendation,

  s.store_id,

  -- epoch seconds -> TIMESTAMP (switch to TIMESTAMP_MILLIS if needed)
  TIMESTAMP_SECONDS(s.time_stamp) AS time_stamp,

  s.user_agent,
  s.user_id_db,
  s.utm_medium,
  s.utm_source,
  s.viewing_product_id
FROM `solid-transport-479213-h5.raw.countly_summary_20251230T165024Z` AS s;
