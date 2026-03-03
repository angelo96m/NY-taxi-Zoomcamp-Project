-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `nytaxi_riders.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nytaxi_bucket_zoomcamp/yellow_tripdata_2024-01.parquet']
);


SELECT COUNT(*) FROM nytaxi_riders.external_yellow_tripdata limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE nytaxi_riders.yellow_tripdata_non_partitioned AS
SELECT * FROM nytaxi_riders.external_yellow_tripdata;

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE nytaxi_riders.yellow_tripdata_partitioned
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM nytaxi_riders.external_yellow_tripdata;

--
SELECT DISTINCT(VendorID)
FROM nytaxi_riders.yellow_tripdata_non_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31';


SELECT DISTINCT(VendorID)
FROM nytaxi_riders.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31';


-- Let's look into the partitions
SELECT table_name, partition_id, total_rows
FROM `nytaxi_riders.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;



-- Creating a partition and cluster table
CREATE OR REPLACE TABLE nytaxi_riders.yellow_tripdata_partitioned_clustered
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM nytaxi_riders.external_yellow_tripdata;



SELECT count(*) as trips
FROM nytaxi_riders.yellow_tripdata_partitioned
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31'
  AND VendorID=1;



SELECT count(*) as trips
FROM nytaxi_riders.yellow_tripdata_partitioned_clustered
WHERE DATE(tpep_pickup_datetime) BETWEEN '2024-01-01' AND '2024-01-31'
  AND VendorID=1;

  