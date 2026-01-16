{{ config(materialized='view') }}

WITH
  src AS (
    SELECT product, to_json_string(product) AS product_json
    FROM `solid-transport-479213-h5`.`raw`.`product`
    WHERE
      to_json_string(product) IS NOT NULL  -- Only process valid JSON strings
  ),
  typed AS (
    SELECT
      -- Extract product_id from the JSON representation
      coalesce(
        json_value(product_json, '$.product.product_id'),
        json_value(product_json, '$.product.productId'),
        json_value(product_json, '$.product.id'),
        json_value(product_json, '$.product._id'))
        AS product_id_str,
      json_value(product_json, '$.product.gender') AS gender,
      json_value(product_json, '$.product.category_name') AS category_name,
      json_value(product_json, '$.product.product_type') AS product_type,
      json_value(product_json, '$.product.store_code') AS store_code,
      json_value(product_json, '$.product.attribute_set') AS attribute_set,
      json_value(product_json, '$.product.category') AS category,
      json_value(product_json, '$.product.material_design') AS material_design,
      json_value(product_json, '$.product.sku') AS sku,
      safe_cast(
        json_value(product_json, '$.product.none_metal_weight') AS numeric)
        AS none_metal_weight,
      safe_cast(
        json_value(product_json, '$.product.fixed_silver_weight') AS numeric)
        AS fixed_silver_weight,
      safe_cast(json_value(product_json, '$.product.gold_weight') AS numeric)
        AS gold_weight
    FROM src
  )
SELECT
  -- deterministic BIGINT surrogate key
  CAST(abs(farm_fingerprint(lower(trim(product_id_str)))) AS int64)
    AS product_key,

  -- keep natural key too (optional but recommended)
  safe_cast(product_id_str AS int64) AS product_id,
  gender,
  category_name,
  none_metal_weight,
  product_type,
  fixed_silver_weight,
  gold_weight,
  store_code,
  attribute_set,
  category,
  material_design,
  sku
FROM typed
WHERE product_id_str IS NOT NULL AND trim(product_id_str) != ''

