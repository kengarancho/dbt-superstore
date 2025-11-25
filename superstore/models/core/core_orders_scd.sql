{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key='order_id'
) }}

-- Core SCD Type 2 for orders using delete+insert incremental strategy.
-- We treat orders as historized by order_date; we keep order-level history and expire prior current record when an update arrives.

{%- if not is_incremental() -%}
select
  {{ dbt_utils.generate_surrogate_key(['order_id','order_date']) }} as order_sk,
  order_id,
  order_date,
  segment,
  state,
  category,
  sub_category,
  product_name,
  sales,
  quantity,
  discount,
  profit,
  cost,
  net_sales,
  order_date as valid_from,
  to_timestamp('9999-12-31 23:59:59') as valid_to,
  true as is_current
from {{ ref('int_orders') }}

{%- else -%}
with incoming as (
  select * from {{ ref('intermediate_orders') }}
),
changed as (
  select distinct order_id from incoming
),
new_rows as (
  select
    {{ dbt_utils.generate_surrogate_key(['order_id','order_date']) }} as order_sk,
    i.order_id,
    i.order_date,
    i.segment,
    i.state,
    i.category,
    i.sub_category,
    i.product_name,
    i.sales,
    i.quantity,
    i.discount,
    i.profit,
    i.cost,
    i.net_sales,
    i.order_date as valid_from,
    to_timestamp('9999-12-31 23:59:59') as valid_to,
    true as is_current
  from incoming i
),
expired_prev as (
  select
    t.order_sk,
    t.order_id,
    t.order_date,
    t.segment,
    t.state,
    t.category,
    t.sub_category,
    t.product_name,
    t.sales,
    t.quantity,
    t.discount,
    t.profit,
    t.cost,
    t.net_sales,
    t.valid_from,
    n.valid_from as valid_to,
    false as is_current
  from {{ this }} t
  join changed c on t.order_id = c.order_id
  join new_rows n on n.order_id = t.order_id
  where t.is_current = true
),
unchanged_history as (
  select * from {{ this }} t
  where t.order_id in (select order_id from changed)
    and t.is_current = false
)

select * from new_rows
union all
select * from expired_prev
union all
select * from unchanged_history

{%- endif -%}
