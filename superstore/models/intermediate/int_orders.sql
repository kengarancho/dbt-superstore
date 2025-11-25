{{ config(materialized='view') }}

-- Intermediate transformations for orders
with src as (
  select * from {{ ref('stg_orders') }}
)

select
  order_id,
  try_to_date(order_date, 'MM/DD/YY') as order_date,
  segment,
  state,
  category,
  sub_category,
  product_name,
  sales,
  quantity,
  discount,
  profit,
  sales - profit as cost,
  {{ net_sales('SALES', 'DISCOUNT') }} as net_sales
from src
where sales is not null
