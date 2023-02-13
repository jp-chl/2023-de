/*
SETUP
*/
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `double-backup-374911.dezoomcamp.external_green_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-jpvr-2023/green/fhv_tripdata_2019-*.csv']
);

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE dezoomcamp.green_tripdata_non_partitoned AS
SELECT * FROM dezoomcamp.external_green_tripdata;


/*
Q1
*/
select count(*) from dezoomcamp.external_green_tripdata;


/*
Q2
*/
-- Count the distinct number of affiliated_base_number
-- for the entire dataset on both the tables.
select count(DISTINCT(e.Affiliated_base_number))
from dezoomcamp.external_green_tripdata e;

select count(DISTINCT(g.Affiliated_base_number))
from dezoomcamp.green_tripdata_non_partitoned g;


/*
Q3
*/
-- 717748
select count(*)
from dezoomcamp.green_tripdata_non_partitoned g
where g.PUlocationID is null
and   g.DOlocationID is null;

-- SELECT SUM(CASE WHEN ((g.PUlocationID IS NULL) AND (g.DOlocationID IS NULL)) THEN 1 ELSE 0 END) AS null_count
-- FROM dezoomcamp.green_tripdata_non_partitoned g;


/*
Q4
*/

-- Cluster on pickup_datetime Cluster on affiliated_base_number
CREATE OR REPLACE TABLE dezoomcamp.green_tripdata_q4_a
CLUSTER BY pickup_datetime, affiliated_base_number
AS
SELECT * FROM dezoomcamp.external_green_tripdata;

-- Partition by pickup_datetime Cluster on affiliated_base_number
CREATE OR REPLACE TABLE dezoomcamp.green_tripdata_q4_b
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number
AS
SELECT * FROM dezoomcamp.external_green_tripdata;

-- Partition by pickup_datetime Partition by affiliated_base_number
-- 
-- This will raise an error beginning with "Only a single PARTITION BY expression is supported but found 2"
-- 
-- https://cloud.google.com/bigquery/docs/partitioned-tables#limitations
-- "BigQuery does not support partitioning by multiple columns. Only one column can be used to partition a table."
CREATE OR REPLACE TABLE dezoomcamp.green_tripdata_q4_c
PARTITION BY DATE(pickup_datetime), affiliated_base_number
AS
SELECT * FROM dezoomcamp.external_green_tripdata;

-- Partition by affiliated_base_number Cluster on pickup_datetime
--
-- This will raise an error beginning with "PARTITION BY expression must be DATE"
--
-- https://cloud.google.com/bigquery/docs/partitioned-tables#limitations
-- BigQuery only supports partitioning by either a "time-unit" or "integer-range" column
CREATE OR REPLACE TABLE dezoomcamp.green_tripdata_q4_d
PARTITION BY affiliated_base_number
CLUSTER BY DATE(pickup_datetime)
AS
SELECT * FROM dezoomcamp.external_green_tripdata;

-- Querying created tables

-- This query will process 647.87 MB when run.
SELECT distinct(Affiliated_base_number)
FROM dezoomcamp.green_tripdata_q4_a -- Clustered on pickup_datetime Cluster on affiliated_base_number
WHERE DATE(pickup_datetime) BETWEEN '2019-02-01' AND '2019-02-28'
order by Affiliated_base_number desc;

-- This query will process 26.41 MB when run.
SELECT distinct(Affiliated_base_number)
FROM dezoomcamp.green_tripdata_q4_b -- Partitioned by pickup_datetime Cluster on affiliated_base_number
WHERE DATE(pickup_datetime) BETWEEN '2019-02-01' AND '2019-02-28'
order by Affiliated_base_number desc;

