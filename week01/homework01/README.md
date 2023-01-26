#### Q1
> Run the command to get help on the "docker build" command. Which tag has the following text? - Write the image ID to the file
```bash
docker --help build | grep "Write the image ID"
```

```
      --iidfile string          Write the image ID to the file
```

#### Q2
> Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash. Now check the python modules that are installed ( use pip list). How many python packages/modules are installed?

```bash
docker run -it --entrypoint "/bin/bash" python:3.9
```

```
Unable to find image 'python:3.9' locally
3.9: Pulling from library/python
bbeef03cda1f: Pull complete
f049f75f014e: Pull complete
56261d0e6b05: Pull complete
9bd150679dbd: Pull complete
5b282ee9da04: Pull complete
03f027d5e312: Pull complete
79903339cfdb: Pull complete
efbad12427dd: Pull complete
862894708010: Pull complete
Digest: sha256:7af616b934168e213d469bff23bd8e4f07d09ccbe87e82c464cacd8e2fb244bf
Status: Downloaded newer image for python:3.9
root@ebac3d1ef9cb:/# pip list
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4
WARNING: You are using pip version 22.0.4; however, version 22.3.1 is available.
You should consider upgrading via the '/usr/local/bin/python -m pip install --upgrade pip' command.
```

> Then 3: pip, setuptools and wheel


#### Q3

Run Postgres and load data as shown in the videos We'll use the green taxi trips from January 2019:

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz

You will also need the dataset with zones:

wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)

```bash
python ingest_data.py \
    --user=root \
    --password=root \
    --host=localhost \
    --port=5432 \
    --db=ny_taxi_v2 \
    --table_name=green_taxi_data \
    --url="green_tripdata_2019-01.csv"
```

```
inserted another chunk, took 9.804 second
inserted another chunk, took 9.702 second
inserted another chunk, took 9.779 second
inserted another chunk, took 9.948 second
inserted another chunk, took 10.099 second
inserted another chunk, took 3.049 second
Finished ingesting data into the postgres database
```

> How many taxi trips were totally made on January 15? (Tip: started and finished on 2019-01-15.)
```sql
select count(*) from green_taxi_data
where extract(year from lpep_dropoff_datetime) = 2019
and extract(month from lpep_dropoff_datetime) = 1
and extract(day from lpep_dropoff_datetime) = 15
and extract(year from lpep_pickup_datetime) = 2019
and extract(month from lpep_pickup_datetime) = 1
and extract(day from lpep_pickup_datetime) = 15;
```

```sql
+-------+
| count |
|-------|
| 20530 |
+-------+
SELECT 1
Time: 0.216s
```

#### Q4


> Which was the day with the largest trip distance Use the pick up time for your calculations.

```sql
SELECT lpep_pickup_datetime, trip_distance
FROM green_taxi_data
order by trip_distance desc
limit 1;
```

```sql
+----------------------+---------------+
| lpep_pickup_datetime | trip_distance |
|----------------------+---------------|
| 2019-01-15 19:27:58  | 117.99        |
+----------------------+---------------+
SELECT 1
Time: 0.101s
```


#### Q5
> In 2019-01-01 how many trips had 2 and 3 passengers?

```sql
select count(*), passenger_count
from green_taxi_data
where extract(year from lpep_pickup_datetime) = 2019
and extract(month from lpep_pickup_datetime) = 1
and extract(day from lpep_pickup_datetime) = 1
and passenger_count in (2,3)
group by passenger_count;
```

```sql
+-------+-----------------+
| count | passenger_count |
|-------+-----------------|
| 1282  | 2               |
| 254   | 3               |
+-------+-----------------+
SELECT 2
Time: 0.085s
```

#### Q6
Tip: Some columns must be enclosed in double quotes

> For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip? We want the name of the zone, not the id.


```sql
SELECT z."Zone"
FROM zones as z
JOIN green_taxi_data as g
on g."DOLocationID" = z."LocationID"
where g."PULocationID" = (select "LocationID" from zones where "Zone" = 'Astoria')
order by g.tip_amount desc limit 1;
```

```sql
+-------------------------------+
| Zone                          |
|-------------------------------|
| Long Island City/Queens Plaza |
+-------------------------------+
SELECT 1
Time: 0.071s
```
