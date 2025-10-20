{{ config(
    materialized='table', 
    schema='dw_oliver'
) }}

SELECT
  cu.cust_key,                 
  d.date_key,                  
  s.store_key,                 
  p.product_key,               
  e.employee_key,              
  ol.quantity,                 
  (ol.quantity * ol.unit_price) AS dollars_sold,
  ol.unit_price
FROM {{ source('oliver_landing','orders') }} o
JOIN {{ source('oliver_landing','orderline') }} ol
  ON ol.order_id = o.order_id
JOIN {{ ref('oliver_dim_customer') }}   cu ON cu.customer_id   = o.customer_id
JOIN {{ ref('oliver_dim_date') }}       d  ON d.date_day      = o.order_date
JOIN {{ ref('oliver_dim_store') }}      s  ON s.store_id      = o.store_id
JOIN {{ ref('oliver_dim_product') }}    p  ON p.product_id    = ol.product_id
LEFT JOIN {{ ref('oliver_dim_employee') }} e ON e.employee_id  = o.employee_id
