{{ config(materialized='table') }}

-- Staging: raw customers from stg source
select
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
    current_timestamp() as ingestion_timestamp
from {{ source('stg', 'customers_raw') }}
