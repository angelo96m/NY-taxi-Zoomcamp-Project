# Module 6: Batch with PySpark 

This folder contains my work for **Module 6** of the Data Engineering Zoomcamp,  
focusing on analytics engineering using **Batch with Spark & PySpark**.

In this module, I processed large-scale NYC Taxi datasets using Apache Spark, transforming raw parquet data into structured and analysis-ready datasets through distributed batch processing.

In this module, I worked with:

- Setting up a local Spark environment using PySpark
- Reading and writing parquet datasets efficiently
- Performing data transformations using Spark DataFrame API and Spark SQL
- Working with timestamps and deriving metrics such as trip duration
- Filtering and cleaning large datasets to handle data quality issues
- Using aggregations and window functions for analytical queries
- Optimizing Spark jobs and understanding execution plans
- Partitioning data for better performance and scalability
- Running batch processing pipelines on large datasets

 ## Contents

### homework/

Homework solution for Module 6, including:

- Notebook with the solution (you can run each cells and see the results)
- README with: questions and answers

### code/

- download_data.sh: download data in a specific folder in your local computer. You need to pass the: TAXI_TYPE: yellow or green and change the YEAR field to download different dataset. 
- Notebooks that you can run cells and see the output. 
- 06_spark_sql.py: it do the same work like the notebook with the same name. 
- 09_spark_gcs.ipynb: Read data from GCS (you need your personal .json credential to connect to GCS). 

