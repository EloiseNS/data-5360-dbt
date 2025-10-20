{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT
  d.date_day as order_date,
  c.first_name || ' ' || c.last_name as customer_name,
  s.store_name,
  p.product_name,
  f.quantity,
  f.unit_price,
  f.dollars_sold
FROM {{ ref('fact_sales') }} f

LEFT JOIN {{ ref('oliver_dim_customer') }} c
  ON f.cust_key    = c.cust_key

LEFT JOIN {{ ref('oliver_dim_date') }} d
  ON f.date_key    = d.date_key

LEFT JOIN {{ ref('oliver_dim_store') }} s
  ON f.store_key   = s.store_key

LEFT JOIN {{ ref('oliver_dim_product') }} p
  ON f.product_key = p.product_key

LEFT JOIN {{ ref('oliver_dim_employee') }} e
  ON f.employee_key = e.employee_key
