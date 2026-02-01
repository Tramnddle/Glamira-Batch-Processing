{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['order_id', 'product_id'],
        on_schema_change='sync_all_columns' --'append_new_columns' -- Thay bằng pp mà mình muốn xử lý thay đổi schema
    )
}}

WITH fact_sales_order__source AS (
    SELECT *
    FROM {{ref('stg_fact_sales_order_tt')}}
    {% if is_incremental() %}
        WHERE order_date >= date_add(current_date(), interval -3 day) --Sửa lại theo column tương ứng của bảng nguồn mà mình muốn filter khi chạy incremental
    {% endif %}
)

,fact_sales_order_rank_timestamp_price AS (
    SELECT 
        *,
        RANK() OVER (PARTITION BY order_id, product_id ORDER BY ip_address, quantity DESC, time_stamp DESC, price DESC) AS RANK
    FROM fact_sales_order__source
    )

,fact_sales_order_remove_dup AS (
    SELECT 
        *
    FROM fact_sales_order_rank_timestamp_price
    WHERE RANK=1
    )


SELECT 
    p1.sk_fact_sales,
    p1.order_id,
    coalesce(p1.product_id,-1) as product_id,
    coalesce(p1.date_id,-1) as date_id, 
    coalesce(p2.location_id,-1) as location_id,
    coalesce(p1.ip_address,"XNA") as ip_address,
    coalesce(p3.sk_user_id,-1) as sk_user_id,
    coalesce(p1.store_id,-1) as store_id,
    local_time,
    order_date,
    p1.quantity,
    p1.price,
    currency,
    p4.rate_to_usd AS rate_to_usd,
    round(p1.price*p1.quantity*p4.rate_to_usd,2) AS line_total_usd
    
FROM fact_sales_order_remove_dup p1
LEFT JOIN {{ref('stg_dim_location')}} AS p2
    ON p1.ip_address = p2.ip_address
LEFT JOIN {{ref('dim_user')}} AS p3
    ON p1.user_id = p3.user_id AND p1.email_address = p3.email_address
LEFT JOIN {{ref('dim_fx_rate_usd')}} p4
    ON p1.currency=p4.from_currency

  