
-- 3. find all the duplicates, understand why they happen, and fix them. 

{{ config(materialized='table') }}

with source as (
    select * from {{ ref('int_trips_unioned') }}
),


find_duplicates as(
    select 
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        trip_distance,
        count(*) as tot_cont
    from source
    group by    
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        trip_distance
    having count(*) > 1 
)

select * from find_duplicates 

-- questa funziona, è stata testata su BigQuery per non creare un'altra query ecc 
/*
select 
    trip_id,
    count(*) as cnt
from {{ ref('fct_trips') }}
group by trip_id
having count(*) > 1
*/