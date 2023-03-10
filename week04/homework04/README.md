## Week 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

This means that in this homework we use the following data [Datasets list](https://github.com/DataTalksClub/nyc-tlc-data/)
* Yellow taxi data - Years 2019 and 2020
* Green taxi data - Years 2019 and 2020 
* fhv data - Year 2019. 

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

---

### Setup

Modify [web_to_gcs.py](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/week_3_data_warehouse/extras/web_to_gcs.py):
- Add these lines to 
```python
web_to_gcs('2019', 'green')
web_to_gcs('2020', 'green')
web_to_gcs('2019', 'yellow')
web_to_gcs('2020', 'yellow')
web_to_gcs('2019', 'fhv')
web_to_gcs('2020', 'fhv')
```

- Set the GCP bucket
```python
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc-data-lake-bucketname")
```

- run the python code
```python
python web_to_gcs.py
```

```bash
Local: green_tripdata_2019-01.csv
Parquet: green_tripdata_2019-01.parquet
GCS: green/green_tripdata_2019-01.parquet

...

Local: fhv_tripdata_2020-12.csv
Parquet: fhv_tripdata_2020-12.parquet
GCS: fhv/fhv_tripdata_2020-12.parquet
```

---

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)?** 

You'll need to have completed the ["Build the first dbt models"](https://www.youtube.com/watch?v=UVI30Vxzd6c) video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

- [] 41648442
- [] 51648442
- [X] 61648442
- [] 71648442

> Answer

After the models are set up (check separate [github repo](https://github.com/jp-chl/2023-de-dbt/blob/develop/models/core/fact_trips.sql)), run:

```bash
dbt run --var 'is_test_run: false'
```

And then query the BigData table filtering:

```sql
SELECT count(*)
FROM `double-backup-374911.dbt_djp.fact_trips`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2020-12-31';
```

```bash
61622055
```

---

### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos?**

You will need to complete "Visualising the data" videos, either using [google data studio](https://www.youtube.com/watch?v=39nLTs74A3E) or [metabase](https://www.youtube.com/watch?v=BnLkrA7a6gM). 

- [] 89.9/10.1
- [] 94/6
- [] 76.3/23.7
- [] 99.1/0.9

---

### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)?**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- [] 33244696
- [X] 43244696
- [] 53244696
- [] 63244696

> Answer

After creating [stg_fhv_tripdata](https://github.com/jp-chl/2023-de-dbt/commit/714779d7eca9c49301c139bcc5dc3077edfb41dd), and [not considering joining with dim_zones](https://github.com/jp-chl/2023-de-dbt/commit/40348164b6992de28cc9892d2a9267482aabbb74) and run ```dbt run --var 'is_test_run: false'```,

Query BG:

```sql
SELECT count(*)
FROM `double-backup-374911.trips_data_all.fhv_tripdata`;
```

```bash
43244696
```

### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)?**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

- [] 12998722
- [X] 22998722
- [] 32998722
- [] 42998722

> Answer

After [joinining with dim_zones](https://github.com/jp-chl/2023-de-dbt/commit/9dec0dd6868204e21fd5f162896b968565c89e64), query BG:


```sql
SELECT count(*)
FROM `double-backup-374911.dbt_djp.fact_fhv_trips`
WHERE DATE(pickup_datetime) BETWEEN '2019-01-01' AND '2019-12-31';
```

```bash
22998722
```

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table?**

Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- [X] January
- December

```sql
SELECT DATE_TRUNC(pickup_datetime, MONTH) AS month,
       COUNT(*) AS count
FROM `double-backup-374911.dbt_djp.fact_fhv_trips`
GROUP BY month;
```

```
2019-01-01 00:00:00 UTC: 19849151
2019-12-01 00:00:00 UTC: 354469
```

## Submitting the solutions

* Form for submitting: https://forms.gle/6A94GPutZJTuT5Y16
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 25 February (Saturday), 22:00 CET


## Solution

We will publish the solution here