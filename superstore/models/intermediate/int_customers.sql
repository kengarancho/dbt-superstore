{{ config(materialized='view') }}

-- Intermediate cleaning and light business logic for customers
with src as (
    select * from {{ ref('stg_customers') }}
)

select
    customer_id,
    initcap(first_name) as first_name,
    initcap(last_name) as last_name,
    lower(email) as email,
    phone,
    address,
    city,
    state,
    created_at,
    updated_at
from src
where customer_id is not null
