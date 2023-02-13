## Week 3 Homework
<b><u>Important Note:</b></u> <p>You can load the data however you would like, but keep the files in .GZ Format. 
If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You can use the CSV option for the GZ files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the fhv 2019 data. </br>
Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). </br>
Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv </p>

> Setup

Upload 2019 data to a GCP bucket.

<p align="center">
  <img src="readme-images/00-setup-bucket.png" width="70%">
</p>

Create the external table
```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `double-backup-374911.dezoomcamp.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-jpvr-2023/green/fhv_tripdata_2019-*.csv']
);
```

---

## Question 1:
What is the count for fhv vehicle records for year 2019?
- [] 65,623,481
- [X] 43,244,696
- [] 22,978,333
- [] 13,942,414

> Answer

<p align="center">
  <img src="readme-images/01-q1.png" width="70%">
</p>

---

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- [] 25.2 MB for the External Table and 100.87MB for the BQ Table
- [] 225.82 MB for the External Table and 47.60MB for the BQ Table
- [] 0 MB for the External Table and 0MB for the BQ Table
- [X] 0 MB for the External Table and 317.94MB for the BQ Table 

> Query external table
```sql
select count(DISTINCT(e.Affiliated_base_number))
from dezoomcamp.external_green_tripdata e;
```

```log
This query will process 0 B when run.
```

<p align="center">
  <img src="readme-images/02-e.png" width="70%">
</p>


>  Query BQ table

```sql
select count(DISTINCT(g.Affiliated_base_number))
from dezoomcamp.green_tripdata_non_partitoned g;
```

```log
This script will process 317.94 MB when run.
```

<p align="center">
  <img src="readme-images/02-bq.png" width="70%">
</p>

---

## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- [X] 717,748
- [] 1,215,687
- [] 5
- [] 20,332

> Answer

```sql
select count(*)
from dezoomcamp.green_tripdata_non_partitoned g
where g.PUlocationID is null
and   g.DOlocationID is null;
```

```log
717748
```

<p align="center">
  <img src="readme-images/03-count.png" width="70%">
</p>

or
```sql
SELECT SUM(CASE WHEN ((g.PUlocationID IS NULL) AND (g.DOlocationID IS NULL)) THEN 1 ELSE 0 END) AS null_count
FROM dezoomcamp.green_tripdata_non_partitoned g;
```

<p align="center">
  <img src="readme-images/03-count-2.png" width="70%">
</p>

---

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- [] Cluster on pickup_datetime Cluster on affiliated_base_number
- [X] Partition by pickup_datetime Cluster on affiliated_base_number
- [] Partition by pickup_datetime Partition by affiliated_base_number
- [] Partition by affiliated_base_number Cluster on pickup_datetime

> Answer

a) Cluster on pickup_datetime Cluster on affiliated_base_number
```sql
CREATE OR REPLACE TABLE dezoomcamp.green_tripdata_q4_a
CLUSTER BY pickup_datetime, affiliated_base_number
AS
SELECT * FROM dezoomcamp.external_green_tripdata;
```

b) Partition by pickup_datetime Cluster on affiliated_base_number
```sql
CREATE OR REPLACE TABLE dezoomcamp.green_tripdata_q4_b
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number
AS
SELECT * FROM dezoomcamp.external_green_tripdata;
```

c) Partition by pickup_datetime Partition by affiliated_base_number

According to [BigQuery's limitations](https://cloud.google.com/bigquery/docs/partitioned-tables#limitations), "_BigQuery does not support partitioning by multiple columns. Only one column can be used to partition a table._".

If you run the following SQL, it will raise the error "```Only a single PARTITION BY expression is supported but found 2```"

```sql
CREATE OR REPLACE TABLE dezoomcamp.green_tripdata_q4_c
PARTITION BY DATE(pickup_datetime), affiliated_base_number
AS
SELECT * FROM dezoomcamp.external_green_tripdata;
```

d) Partition by affiliated_base_number Cluster on pickup_datetime

According to [BigQuery's limitations](https://cloud.google.com/bigquery/docs/partitioned-tables#limitations), BigQuery only supports partitioning by either a "time-unit" or "integer-range" column.

If you run the following SQL, it will raise an error beginning with "```PARTITION BY expression must be DATE```"

```sql
CREATE OR REPLACE TABLE dezoomcamp.green_tripdata_q4_d
PARTITION BY affiliated_base_number
CLUSTER BY DATE(pickup_datetime)
AS
SELECT * FROM dezoomcamp.external_green_tripdata;
```

Querying the created tables:

```sql
-- This query will process 647.87 MB when run.
SELECT distinct(Affiliated_base_number)
FROM dezoomcamp.green_tripdata_q4_a -- Clustered on pickup_datetime Cluster on affiliated_base_number
WHERE DATE(pickup_datetime) BETWEEN '2019-02-01' AND '2019-02-28'
order by Affiliated_base_number desc;
```

<p align="center">
  <img src="readme-images/04-a.png" width="70%">
</p>

```sql
-- This query will process 26.41 MB when run.
SELECT distinct(Affiliated_base_number)
FROM dezoomcamp.green_tripdata_q4_b -- Partitioned by pickup_datetime Cluster on affiliated_base_number
WHERE DATE(pickup_datetime) BETWEEN '2019-02-01' AND '2019-02-28'
order by Affiliated_base_number desc;
```

<p align="center">
  <img src="readme-images/04-b.png" width="70%">
</p>

> So the best strategy, to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number, is to partition by pickup_datetime and cluster on affiliated_base_number.

---

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 2019/03/01 and 2019/03/31 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

---

## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Container Registry
- Big Table

---

## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

---

## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 


Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur. 
