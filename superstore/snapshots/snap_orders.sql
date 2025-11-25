{% snapshot snap_orders %}
    {{ config(
      target_schema='snapshots',
      unique_key='order_id',
      strategy='check',
      check_cols=['order_date', 'segment', 'state', 'category', 'sub_category', 'product_name', 'sales', 'quantity', 'discount', 'profit']
    ) }}

    select * from {{ ref('int_orders') }}
{% endsnapshot %}
