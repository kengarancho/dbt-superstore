{% macro net_sales(sales_col, discount_col) -%}
{#
  Return a SQL expression that calculates net sales as: sales * (1 - discount)
  Accepts column names as strings (e.g. 'SALES', 'DISCOUNT').
  Uses the adapter to safely quote identifiers.
#}
  {{ return(adapter.quote(sales_col) ~ ' * (1 - ' ~ adapter.quote(discount_col) ~ ')') }}
{%- endmacro %}
