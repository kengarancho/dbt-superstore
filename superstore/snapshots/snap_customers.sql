{% snapshot snap_customers %}
    {{ config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='check',
      check_cols=['first_name', 'last_name', 'email', 'phone', 'address', 'city', 'state', 'created_at', 'updated_at']
    ) }}

    select * from {{ ref('int_customers') }}
{% endsnapshot %}
