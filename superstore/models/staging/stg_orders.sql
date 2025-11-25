{{ config(materialized='table') }}

-- Staging: raw orders from stg source
select
    ORDER_ID as order_id,
    ORDER_DATE as order_date,
    SEGMENT,
    STATE,
    CATEGORY,
    SUB_CATEGORY,
    PRODUCT_NAME,
    SALES,
    QUANTITY,
    DISCOUNT,
    PROFIT,
    current_timestamp() as ingestion_timestamp
from {{ source('stg', 'orders_raw') }}
