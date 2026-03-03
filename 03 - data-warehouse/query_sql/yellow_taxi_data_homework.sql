-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `yellow_taxi_homework.external_yellow_tripdata`
  OPTIONS (
    format = 'PARQUET',
    uris =
      [
        'gs://nytaxi_bucket_zoomcamp/yellow_tripdata_2024-01.parquet',
        'gs://nytaxi_bucket_zoomcamp/yellow_tripdata_2024-02.parquet',
        'gs://nytaxi_bucket_zoomcamp/yellow_tripdata_2024-03.parquet',
        'gs://nytaxi_bucket_zoomcamp/yellow_tripdata_2024-04.parquet',
        'gs://nytaxi_bucket_zoomcamp/yellow_tripdata_2024-05.parquet',
        'gs://nytaxi_bucket_zoomcamp/yellow_tripdata_2024-06.parquet']);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE yellow_taxi_homework.yellow_tripdata_non_partitioned
AS
SELECT * FROM `yellow_taxi_homework.external_yellow_tripdata`;


select * from `yellow_taxi_homework.yellow_tripdata_non_partitioned` limit 10;


-- question 1: What is count of records for the 2024 Yellow Taxi Data?
SELECT COUNT(*)
FROM `yellow_taxi_homework.yellow_tripdata_non_partitioned`
where date(tpep_pickup_datetime) BETWEEN date('2024-01-01') and date('2024-12-31');
--answer: C : 20332057 


-- question 2: Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables. 

-- query on external table: 
select distinct(PULocationID) 
from `yellow_taxi_homework.external_yellow_tripdata`; 
--estimed 0 B 

-- query on the Table: 
select distinct(PULocationID) 
from `yellow_taxi_homework.yellow_tripdata_non_partitioned`; 
--estimed 155,12 B

-- answer: B : 0 MB for the External Table and 155.12 MB for the Materialized Table


-- question 3: 
select PULocationID
from `yellow_taxi_homework.yellow_tripdata_non_partitioned`; 
-- 155,12 MB 

select PULocationID, DOLocationID  
from `yellow_taxi_homework.yellow_tripdata_non_partitioned`; 
-- 310,24 MB 

-- answer: A 


-- question 4: How many records have a fare_amount of 0? 
select count(*) 
from `yellow_taxi_homework.yellow_tripdata_non_partitioned`
where fare_amount = 0.0; 
--8,333 
-- answer: D 


-- question 5: 
CREATE OR REPLACE TABLE yellow_taxi_homework.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `yellow_taxi_homework.external_yellow_tripdata`; 
--answer: A : Partition by tpep_dropoff_datetime and Cluster on VendorID


-- question 6: 

--for non partitioned table
SELECT COUNT(VendorID)
FROM `yellow_taxi_homework.yellow_tripdata_non_partitioned`
where date(tpep_dropoff_datetime) BETWEEN date('2024-03-01') and date('2024-03-15');
-- 310,24 MB 

--for partitioned table
SELECT COUNT(VendorID)
FROM `yellow_taxi_homework.yellow_tripdata_partitioned_clustered`
where date(tpep_dropoff_datetime) BETWEEN date('2024-03-01') and date('2024-03-15');

--answer: B : 310.24 MB for non-partitioned table and 26.84 MB for the partitioned table


-- question 7: Where is the data stored in the External Table you created? 
-- answer: C: GCP Bucket 


-- question 8: It is best practice in Big Query to always cluster your data: 
-- answer: B: False 


--question 9: 
SELECT COUNT(*)
FROM `yellow_taxi_homework.yellow_tripdata_non_partitioned`; 
-- 0 B 
