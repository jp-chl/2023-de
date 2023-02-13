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