# Module 4: Analytics Engineering with dbt

This folder contains my work for **Module 4** of the Data Engineering Zoomcamp,  
focusing on analytics engineering using **dbt Cloud + BigQuery**.

In this module, I transformed raw NYC Taxi datasets into analytics-ready models using dbt, following a layered transformation approach.

In this module, I worked with:

- Connecting dbt Cloud to BigQuery
- Structuring a dbt project (staging → intermediate → marts)
- Building fact and dimension models
- Writing generic and custom data tests
- Understanding lineage and DAG execution
- Running production deployment jobs
- Extending the warehouse with additional FHV data

## Architecture Overview

This module builds on top of the BigQuery warehouse created in Module 3.

Flow:

GCS → BigQuery (raw tables) → dbt (transformations) → Analytics-ready marts

Layers implemented in dbt:

- **Staging models**  
  Clean, standardize, and cast raw taxi data

- **Intermediate models**  
  Combine Green and Yellow datasets and apply business logic

- **Marts (Fact & Dimension models)**  
  - `fct_trips`
  - `fct_monthly_zone_revenue`
  - `dim_zones`
  - `dim_vendors`

 ## Contents

### homework/

Homework solution for Module 4, including:

- Production deployment job configuration
- SQL queries for quiz questions
- Implementation of FHV 2019 staging model

### dbt_taxi_rides_ny/

Complete dbt project used in this module.

Includes:

- `models/`
  - staging/
  - intermediate/
  - marts/
- `macros/`
- `seeds/`
- `packages.yml`
- `dbt_project.yml`
- 
This project was originally developed in dbt Cloud and later exported to this repository for reproducibility.

### scripts/

Python utilities used to extend the warehouse with additional datasets:

- `load_fhv_data.py`  
  Downloads FHV 2019 data, decompresses it, and uploads to GCS.

- `load_nytaxi_data.py`  
Downloads NY taxi data for Green and Yellow data trip for 2019 and 2020 year. 
