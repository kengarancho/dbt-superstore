SELECT
    ORDER_ID,
    ORDER_DATE,
    SEGMENT,
    STATE,
    CATEGORY,
    SUB_CATEGORY,
    PRODUCT_NAME,
    SALES,
    QUANTITY,
    DISCOUNT,
    PROFIT
FROM {{ source('bronze', 'orders_raw') }}
