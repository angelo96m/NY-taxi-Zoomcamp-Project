{% macro map_payment_type(payment_type) -%}
CASE
    WHEN {{ payment_type }} = 1 THEN 'Cash'
    WHEN {{ payment_type }} = 2 THEN 'Credit Card'
    WHEN {{ payment_type }} = 3 THEN 'PayPal'
    WHEN {{ payment_type }} IS NULL THEN 'Unknown'
    ELSE 'Other'
END
{%- endmacro %}