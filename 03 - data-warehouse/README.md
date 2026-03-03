# Module 3: Data Warehousing & BigQuery

This folder contains my work for **Module 3** of the Data Engineering Zoomcamp,
focusing on building and optimizing a data warehouse using Google BigQuery.

In this module, I worked with:
- Loading large-scale NYC Taxi datasets into Google Cloud Storage
- Creating external tables in BigQuery from GCS data
- Building materialized (managed) tables in BigQuery
- Understanding columnar storage and how it affects query cost
- Using partitioning and clustering to optimize query performance
- Analyzing query execution and bytes scanned in BigQuery

## Contents

load_yellow_taxi_data.py: 

Python scripts used to load NYC Yellow Taxi data (Jan 2024 - Jun 2024) into GCS. 


### query_sql/
BigQuery SQL scripts used throughout the module:

- [`bigquery_nytaxi.sql`]
  External → non-partitioned → partitioned → partitioned & clustered tables
  for Yellow Taxi data (Jan 2024). 

- [`bigquery_ml_models.sql`]
  BigQuery ML example for predicting taxi tips. 

- [`yellow_taxi_data_homework.sql`]
  Homework solution for Module 3:
  
- Loading Yellow Taxi Trip data (Jan–Jun 2024) into GCS using Python
- Creating external and materialized BigQuery tables
- Query cost estimation and optimization analysis
- Partitioning and clustering experiments 
