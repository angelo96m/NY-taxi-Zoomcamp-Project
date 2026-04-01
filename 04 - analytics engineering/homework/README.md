# Module 4 Homework: Analytics Engineering with dbt

This homework demonstrates how I used **dbt Cloud + BigQuery** to transform raw NYC taxi trip data into analytics-ready models.

The project follows a layered dbt structure:
- Staging models – clean and standardize raw Green and Yellow taxi data  
- Intermediate models – combine datasets and apply business logic  
- Marts (fact & dimension models) – build analytics-ready tables  
- Tests & documentation – ensure data quality and model integrity  

## Setup

This homework was completed using the Cloud setup: 
- BigQuery as the warehouse 
- dbt Cloud for development and runs

### BigQuery prerequisites

Make sure the following are ready in GCP:

- BigQuery API enabled
- A dataset created for the course (e.g. nytaxi)
- Raw tables available in the dataset:
  - green_tripdata (2019–2020)
  - yellow_tripdata (2019–2020)


### dbt Cloud connection (BigQuery)

In dbt Cloud, I created a BigQuery connection using a service account with permissions to:
- read from the raw dataset (e.g. nytaxi)
- create models in the dbt target dataset (e.g. dbt_<username>)


## Questions

### Question 1. dbt Lineage and Execution

Given a dbt project with the following structure:

```
models/
├── staging/
│   ├── stg_green_tripdata.sql
│   └── stg_yellow_tripdata.sql
└── intermediate/
    └── int_trips_unioned.sql (depends on stg_green_tripdata & stg_yellow_tripdata)
```

If you run `dbt run --select int_trips_unioned`, what models will be built?

- `stg_green_tripdata`, `stg_yellow_tripdata`, and `int_trips_unioned` (upstream dependencies)
- Any model with upstream and downstream dependencies to `int_trips_unioned`
- `int_trips_unioned` only 
- `int_trips_unioned`, `int_trips`, and `fct_trips` (downstream dependencies)


**Solution:** 
`int_trips_unioned` only 


### Question 2. dbt Tests

You've configured a generic test like this in your `schema.yml`:

```yaml
columns:
  - name: payment_type
    data_tests:
      - accepted_values:
          arguments:
            values: [1, 2, 3, 4, 5]
            quote: false
```

Your model `fct_trips` has been running successfully for months. A new value `6` now appears in the source data.

What happens when you run `dbt test --select fct_trips`?

- dbt will skip the test because the model didn't change
- dbt will fail the test, returning a non-zero exit code 
- dbt will pass the test with a warning about the new value
- dbt will update the configuration to include the new value

**Solution:**
dbt will fail the test, returning a non-zero exit code 


### Question 3. Counting Records in `fct_monthly_zone_revenue`

After running your dbt project, query the `fct_monthly_zone_revenue` model.

What is the count of records in the `fct_monthly_zone_revenue` model?

- 12,998
- 14,120
- 12,184 
- 15,421

**Solution:**
12,184 

NOTE: Run the following query in BigQuery: 
```sql
select count(*)
from `dbt_prod.fct_monthly_zone_revenue`;
```

### Question 4. Best Performing Zone for Green Taxis (2020)

Using the `fct_monthly_zone_revenue` table, find the pickup zone with the **highest total revenue** (`revenue_monthly_total_amount`) for **Green** taxi trips in 2020.

Which zone had the highest revenue?

- East Harlem North
- Morningside Heights
- East Harlem South
- Washington Heights South

**Solution:**
East Harlem North

NOTE: Run the following query in BigQuery:
```sql 
select
    look.zone,
    sum(fare_amount) as total_revenue
from `dbt_prod.stg_green_tripdata` as green
  join `dbt_prod.taxi_zone_lookup` as look on green.pickup_location_id = look.locationId
where DATE(pickup_datetime) BETWEEN '2020-01-01' AND '2020-12-31'
group by look.zone
order by total_revenue desc
limit 1;
```

### Question 5. Green Taxi Trip Counts (October 2019)

Using the `fct_monthly_zone_revenue` table, what is the **total number of trips** (`total_monthly_trips`) for Green taxis in October 2019?

- 500,234
- 350,891
- 384,624 
- 421,509

**Solution:**
384,624 

NOTE: Run the following query in BigQuery:
```sql 
select
    sum(total_monthly_trips) as total_trips
from `dbt_prod.fct_monthly_zone_revenue`
where service_type = 'Green'
  and extract(year from revenue_month) = 2019
  and extract(month from revenue_month) = 10;
```

### Question 6. Build a Staging Model for FHV Data

Create a staging model for the **For-Hire Vehicle (FHV)** trip data for 2019.

1. Load the [FHV trip data for 2019](https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv) into your data warehouse
2. Create a staging model `stg_fhv_tripdata` with these requirements:
   - Filter out records where `dispatching_base_num IS NULL`
   - Rename fields to match your project's naming conventions (e.g., `PUlocationID` → `pickup_location_id`)

What is the count of records in `stg_fhv_tripdata`?

- 42,084,899
- 43,244,693 
- 22,998,722 
- 44,112,187


**Solution:**
43,244,693 

NOTE: Run the following query in BigQuery:
```sql 
select count(*) 
from `dbt_prod.external_fhv_tripdata
```

