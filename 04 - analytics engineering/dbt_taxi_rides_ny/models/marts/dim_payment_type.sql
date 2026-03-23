-- 4. find a way to enrich the column payment_type. 

with payment_type_query as(
    select * from {{ ref('int_trips_unioned') }}
)

select distinct
    payment_type,
    {{ map_payment_type('payment_type') }} as payment_type_name
from payment_type_query