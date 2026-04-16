# Workshop Homework: dlt (data load tool)

Built a custom dlt pipeline to load NYC Yellow Taxi trip data from a paginated REST API into DuckDB.

Project location: [dlt-workshop/taxi-pipeline]

## Data Source

| Property | Value |
|----------|-------|
| Base URL | `https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api` |
| Format | Paginated JSON |
| Page Size | 1,000 records per page |
| Pagination | Stop when an empty page is returned |

## Setup

### Step 1: Create Project

```bash
mkdir taxi-pipeline
cd taxi-pipeline
```

### Step 2: Set Up dlt MCP Server

Added the dlt MCP server in Cursor (Settings → Tools & MCP → New MCP Server):

```json
{
  "mcpServers": {
    "dlt": {
      "command": "uv",
      "args": [
        "run",
        "--with", "dlt[duckdb]",
        "--with", "dlt-mcp[search]",
        "python", "-m", "dlt_mcp"
      ]
    }
  }
}
```

### Step 3: Install dlt

```bash
pip install "dlt[workspace]"
```

### Step 4: Initialize Project

```bash
dlt init dlthub:taxi_pipeline duckdb
```

### Step 5: Prompt the Agent

Used Cursor agent to build the pipeline with the following prompt:

```
Build a REST API source for NYC taxi data.

API details:
- Base URL: https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api
- Data format: Paginated JSON (1,000 records per page)
- Pagination: Stop when an empty page is returned

Place the code in taxi_pipeline.py and name the pipeline taxi_pipeline.
```

Result: [`taxi_pipeline.py`]

| Config | Value |
|--------|-------|
| Pipeline name | `taxi_pipeline` |
| Destination | DuckDB (`taxi_pipeline.duckdb`) |
| Dataset | `taxi_data` |
| Write Disposition | `replace` |

### Step 6: Run the Pipeline

```bash
cd taxi-pipeline
uv run taxi_pipeline.py
```

Pipeline fetched 10 pages (10,000 records total).

## Exploring the Data

### dlt Dashboard

```bash
uv run dlt pipeline taxi_pipeline show
```

## Questions

### Question 1: What is the start date and end date of the dataset?

- 2009-01-01 to 2009-01-31
- 2009-06-01 to 2009-07-01 
- 2024-01-01 to 2024-02-01
- 2024-06-01 to 2024-07-01

```sql
SELECT
  MIN(CAST(trip_pickup_date_time AS DATE)) AS start_date,
  MAX(CAST(trip_pickup_date_time AS DATE)) AS end_date
FROM taxi_trips;
```

**Solution:** 2009-06-01 to 2009-07-01 

![Q1 date range query result](https://github.com/angelo96m/docker-workshop/blob/main/images/dlt_workshop_q1.png)

### Question 2: What proportion of trips are paid with credit card?

- 16.66%
- 26.66% 
- 36.66%
- 46.66%

```sql
SELECT
  ROUND(
    100.0 * SUM(CASE WHEN payment_type = 'Credit' THEN 1 ELSE 0 END) / COUNT(*),
    2
  ) AS credit_card_percentage
FROM taxi_trips;
```

**Solution:** 26.66% 

![Q2 credit card percentage query result](https://github.com/angelo96m/docker-workshop/blob/main/images/dlt_workshop_q2.png)

### Question 3: What is the total amount of money generated in tips?

- $4,063.41
- $6,063.41 
- $8,063.41
- $10,063.41

```sql
SELECT
  ROUND(SUM(tip_amt), 2) AS total_tips
FROM taxi_trips;
```

**Solution:** $6,063.41 

![Q3 total tips query result](https://github.com/angelo96m/docker-workshop/blob/main/images/dlt_workshop_q3.png)
