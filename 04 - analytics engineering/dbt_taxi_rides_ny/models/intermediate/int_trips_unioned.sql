with green_tripdata as (
    select * from {{ref('stg_green_tripdata') }}
),
/*
    In the query I did a union with green and yellow table for NY taxi
*/
--ref = reference 

yellow_tripdata as (
    select * from {{ref('stg_yellow_tripdata')}}
),

trip_unioned as(
    select * from green_tripdata
    union all 
    select * from yellow_tripdata
)
/*
Exemple:
select 
    vendor_id, 
    count(*) as tot_vendor
from trip_unioned
group by vendor_id
order by vendor_id
*/

select *
from trip_unioned