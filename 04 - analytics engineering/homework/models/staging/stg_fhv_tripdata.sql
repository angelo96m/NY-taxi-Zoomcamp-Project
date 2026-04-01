select
    -- identifiers
    cast(dispatching_base_num as string) as dispatching_base_num,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    -- location ids
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,

    -- sr flag
    cast(sr_flag as integer) as sr_flag

from {{ source ("bigquery_data", "external_fhv_tripdata") }} 
where dispatching_base_num is not null
