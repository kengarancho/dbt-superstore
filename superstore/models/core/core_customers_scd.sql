{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key='customer_id'
) }}

-- Core SCD Type 2 for customers using delete+insert incremental strategy.
-- Columns: customer_sk, customer_id, first_name, last_name, email, phone, address, city, state,
-- created_at, updated_at, valid_from, valid_to, is_current

{% set cols = [
  'customer_sk','customer_id','first_name','last_name','email','phone','address','city','state',
  'created_at','updated_at','valid_from','valid_to','is_current'
] %}

{%- if not is_incremental() -%}
-- Full-refresh: create a single current record per customer
select
  {{ dbt_utils.generate_surrogate_key(['customer_id','updated_at']) }} as customer_sk,
  customer_id,
  first_name,
  last_name,
  email,
  phone,
  address,
  city,
  state,
  created_at,
  updated_at,
  updated_at as valid_from,
  to_timestamp('9999-12-31 23:59:59') as valid_to,
  true as is_current
from {{ ref('int_customers') }}

{%- else -%}
-- Incremental run: for changed customers, delete+insert will replace all rows for that customer_id
with incoming as (
  select * from {{ ref('intermediate_customers') }}
),
changed as (
  select distinct customer_id from incoming
),
-- New current rows for incoming customers
new_rows as (
  select
    {{ dbt_utils.generate_surrogate_key(['customer_id','updated_at']) }} as customer_sk,
    i.customer_id,
    i.first_name,
    i.last_name,
    i.email,
    i.phone,
    i.address,
    i.city,
    i.state,
    i.created_at,
    i.updated_at,
    i.updated_at as valid_from,
    to_timestamp('9999-12-31 23:59:59') as valid_to,
    true as is_current
  from incoming i
),
-- Expire the previous current row for incoming customers (if any)
expired_prev as (
  select
    t.customer_sk,
    t.customer_id,
    t.first_name,
    t.last_name,
    t.email,
    t.phone,
    t.address,
    t.city,
    t.state,
    t.created_at,
    t.updated_at,
    t.valid_from,
    n.valid_from as valid_to,
    false as is_current
  from {{ this }} t
  join changed c on t.customer_id = c.customer_id
  join new_rows n on n.customer_id = t.customer_id
  where t.is_current = true
),
-- Preserve older history rows for incoming customers (unchanged historical rows)
unchanged_history as (
  select * from {{ this }} t
  where t.customer_id in (select customer_id from changed)
    and t.is_current = false
)

select * from new_rows
union all
select * from expired_prev
union all
select * from unchanged_history

{%- endif -%}
