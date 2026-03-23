/*
    To Do: 
    1. one row per trip (doesn't matter if yellow or green)
    2. add a primary key (trip_id).It has to be unique.
    3. find all the duplicates, understand why they happen, and fix them. 
    4. find a way to enrich the column payment_type. 
*/

-- 1. one row per trip (doesn't matter if yellow or green)
/*
with trips_unioned_homework as(
    select * from {{ ref('int_trips_unioned') }}
),

trips_homework as (
    select * from trips_unioned_homework 
    limit 1 
)

select * from trips_homework 
*/ 

-- 2. add a primary key (trip_id).It has to be unique.

{{ config(materialized='table') }} --create a table on BigQuery

with source as (
    select * from {{ ref('int_trips_unioned') }}
),

add_pk as (
    select
        /*funzionalità per generare una key univoca (surrogate pattern)
        to use dbt_utils.generate_surrogate_key we need use: dbt_utils, 
        before to use this we need create the package.yml and via terminal: 
        dbt deps
        */
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id',
            'pickup_datetime',
            'dropoff_datetime',
            'trip_distance'
        ]) }} as trip_id,
        *
    from source
)

select * from add_pk

-- 3. find all the duplicates, understand why they happen, and fix them. 

-- query realizzata nel file: fct_duplicates.sql 


-- 4. find a way to enrich the column payment_type. 

/*
    query realizzata nel file: dim_payment_type.sql 
*/