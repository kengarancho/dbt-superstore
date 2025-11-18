{% snapshot orders_snapshot %}

{{
    config(
        target_schema='SNAPSHOTS',
        unique_key='ORDER_ID',
        strategy='timestamp',
        updated_at='ORDER_DATE'
    )
}}

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
FROM {{ ref('silver_orders') }}

{% endsnapshot %}
