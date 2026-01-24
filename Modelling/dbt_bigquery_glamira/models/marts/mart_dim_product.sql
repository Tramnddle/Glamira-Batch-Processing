{{ config(materialized='table') }}

select
  product_key,
  product_id,
  sku,
  gender,
  category_name,
  product_type,
  store_code,
  attribute_set,
  category,
  material_design
from {{ ref('stg_product') }}
