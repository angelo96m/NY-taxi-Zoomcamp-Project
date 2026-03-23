with taxi_zone_lookup as(
    select * from {{ref('taxi_zone_lookup') }}
)

renamed as(
    select
        locationID as location_id,
        borough,
        zone, 
        service_zone
)

select * from renamed
