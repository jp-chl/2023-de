#### Setup

```bash
git clone https://github.com/discdiver/prefect-zoomcamp.git
```



#### Q1

> Load January 2020 data
Using the etl_web_to_gcs.py flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

- [X] 447,770
- [] 766,792
- [] 299,234
- [] 822,132

> Answer

For green dataset, a minor change is required
<p align="center">
  <img src="readme-images/setup-01.png" width="70%">
</p>

Then, run python code:
<p align="center">
  <img src="readme-images/01-output.png" width="70%">
</p>

---

#### Q2
> Cron is a common scheduling specification for workflows. Using the flow in etl_web_to_gcs.py, create a deployment to run on the first of every month at 5am UTC.

What’s the cron schedule for that?

- [X] 0 5 1 * *
- [] 0 0 5 1 *
- [] 5 * 1 0 *
- [] * * 5 1 0

> Answer

- 0: Minute: The command will run at minute 0, i.e., the top of the hour.
- 5: Hour: The command will run at 5 AM.
- 1: Day of the Month: The command will run on the first day of every month.
- \*: The command will run every month.
- \*: The command will run every day of the week.

```bash
prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs -n "1st deployment" --cron "0 5 1 * *"
```

```
Found flow 'etl-web-to-gcs'
Default '.prefectignore' file written to /home/jp/github/prefect-zoomcamp/flows/02_gcp/.prefectignore
Deployment YAML created at '/home/jp/github/prefect-zoomcamp/flows/02_gcp/etl_web_to_gcs-deployment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass --skip-upload to suppress this warning.
```

Last command generates the yaml file ```etl_web_to_gcs-deployment.yaml```:
```yaml
###
### A complete description of a Prefect Deployment for flow 'etl-web-to-gcs'
###
name: 1st deployment
description: The main ETL function
version: c32c546ce2bd947efe09556ab96caadb
# The work queue that will handle this deployment's runs
work_queue_name: default
tags: []
parameters: {}
schedule:
  cron: 0 5 1 * *
  timezone: null
  day_or: true
infra_overrides: {}
infrastructure:
  type: process
  env: {}
  labels: {}
  name: null
  command: null
  stream_output: true
  working_dir: null
  block_type_slug: process
  _block_type_slug: process

###
### DO NOT EDIT BELOW THIS LINE
###
flow_name: etl-web-to-gcs
manifest_path: null
storage: null
path: /home/jp/github/prefect-zoomcamp/flows/02_gcp
entrypoint: etl_web_to_gcs.py:etl_web_to_gcs
parameter_openapi_schema:
  title: Parameters
  type: object
  properties: {}
  required: null
  definitions: null
```

Finally, apply the deployment
```bash
prefect deployment apply etl_web_to_gcs-deployment.yaml 
```

```
Successfully loaded '1st deployment'
Deployment 'etl-web-to-gcs/1st deployment' successfully created with id '5193164b-5dda-4ed4-9a26-855bae811c62'.
View Deployment in UI: http://127.0.0.1:4200/deployments/deployment/5193164b-5dda-4ed4-9a26-855bae811c62

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
```

In the Web UI:
<p align="center">
  <img src="readme-images/02-1st-deploy.png" width="70%">
</p>

---

#### Q3
> Using etl_gcs_to_bq.py as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table.

How many rows did your flow code process?

- [X] 14,851,920
- [] 12,282,990
- [] 27,235,753
- [] 11,338,483

> Answer

Modify the [etl_gcs_to_bq.py](./parameterized_etl_gcs_to_bq.py) script to not transform the data, and print the total records processed.

e.g.

```python
@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"Skipping cleaning")
    return df
```

Then, create a deployment:
```bash
prefect deployment build ./parameterized_etl_gcs_to_bq.py:etl_parent_flow -n "Q3 Parameterized ETL"
```

```
Found flow 'etl-parent-flow'
Default '.prefectignore' file written to 
/home/jp/github/prefect-zoomcamp/flows/03_deployments/.prefectignore
Deployment YAML created at 
'/home/jp/github/prefect-zoomcamp/flows/03_deployments/etl_parent_flow-deployment.yaml'.
Deployment storage None does not have upload capabilities; no files uploaded.  Pass --skip-upload to 
suppress this warning.
```

Then, apply the deployment:
```bash
prefect deployment apply etl_parent_flow-deployment.yaml 
```

```
Successfully loaded 'Q3 Parameterized ETL'
Deployment 'etl-parent-flow/Q3 Parameterized ETL' successfully created with id '9caf94fd-1ccc-4c62-96a4-9d639fc0f81f'.
View Deployment in UI: http://127.0.0.1:4200/deployments/deployment/9caf94fd-1ccc-4c62-96a4-9d639fc0f81f

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
```

Deployment is available through the Web UI:
<p align="center">
  <img src="readme-images/03-parameterized-deploy.png" width="70%">
</p>

If you click over Run, the deployment is scheduled but it does not run inmediately.

<p align="center">
  <img src="readme-images/03-run.png" width="30%">
</p>

<p align="center">
  <img src="readme-images/03-run-delayed.png" width="70%">
</p>

As soon as you run the agent, the deployment is executed

<p align="center">
  <img src="readme-images/03-agent.png" width="70%">
</p>

<p align="center">
  <img src="readme-images/03-run-executing.png" width="70%">
</p>

<p align="center">
  <img src="readme-images/03-run-completed.png" width="70%">
</p>

Total records processed printed in logs:
<p align="center">
  <img src="readme-images/03-run-output.png" width="70%">
</p>


Records processed in BG:
<p align="center">
  <img src="readme-images/03-bq.png" width="70%">
</p>

---

#### Q4
> Using the web_to_gcs script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

- [] 88,019
- [] 192,297
- [X] 88,605
- [] 190,225

> Answer

- 1: Create a Github Block (check [deploy-from-github.py](./deploy-from-github.py))
- 2: Create a deployment with the ```--sb``` parameter

```bash
python deploy-from-github.py
```

```log
15:58:42.490 | INFO    | prefect.engine - Created flow run 'fresh-hog' for flow 'main-flow'
15:58:42.662 | INFO    | Flow run 'fresh-hog' - Created task run 'load_block_from_github-0' for task 'load_block_from_github'
15:58:42.663 | INFO    | Flow run 'fresh-hog' - Executing 'load_block_from_github-0' immediately...
15:58:43.270 | INFO    | Task run 'load_block_from_github-0' - Finished in state Completed()
15:58:43.307 | INFO    | Flow run 'fresh-hog' - Finished in state Completed('All states completed.')
```

In the Web UI a new block has created

<p align="center">
  <img src="readme-images/03-block-github.png" width="70%">
</p>

> Use that block in a deployment:

```bash
prefect deployment build ./week02/homework02/etl_web_to_gcs.py:etl_web_to_gcs --name test_q3_etl_web_to_gcs -sb github/test --apply
```

```log
Found flow 'etl-web-to-gcs'
Deployment YAML created at 
'/home/jp/github/prefect-zoomcamp/flows/03_deployments/etl_web_to_gcs-deployment.yaml'.
Deployment storage GitHub(repository='https://github.com/jp-chl/2023-de/', reference=None, access_token=None,
include_git_objects=True) does not have upload capabilities; no files uploaded.  Pass --skip-upload to 
suppress this warning.
Deployment 'etl-web-to-gcs/test_q3_etl_web_to_gcs' successfully created with id 
'6baf8f36-a72c-4e0b-b1e3-72d5911d4e08'.

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
```

> Running the deployment:

```log
15:52:22.123 | INFO    | prefect.agent - Submitting flow run 'a88c3293-d67a-4a22-b2e3-22f2c4e8c624'
15:52:22.205 | INFO    | prefect.infrastructure.process - Opening process 'vivacious-alligator'...
15:52:22.239 | INFO    | prefect.agent - Completed submission of flow run 'a88c3293-d67a-4a22-b2e3-22f2c4e8c624'
/home/jp/anaconda3/envs/prefect-dev/lib/python3.9/runpy.py:127: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
  warn(RuntimeWarning(msg))
15:52:25.037 | INFO    | Flow run 'vivacious-alligator' - Downloading flow code from storage at ''
15:52:25.624 | INFO    | Flow run 'vivacious-alligator' - dataset_url: [https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz]
15:52:25.663 | INFO    | Flow run 'vivacious-alligator' - Created task run 'fetch-0' for task 'fetch'
15:52:25.664 | INFO    | Flow run 'vivacious-alligator' - Executing 'fetch-0' immediately...
week02/homework02/etl_web_to_gcs.py:15: DtypeWarning: Columns (3) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv(dataset_url)
15:52:26.227 | INFO    | Task run 'fetch-0' - Finished in state Completed()
15:52:26.264 | INFO    | Flow run 'vivacious-alligator' - Created task run 'clean-0' for task 'clean'
15:52:26.265 | INFO    | Flow run 'vivacious-alligator' - Executing 'clean-0' immediately...
15:52:26.385 | INFO    | Task run 'clean-0' -    VendorID lpep_pickup_datetime  ... trip_type congestion_surcharge
0       2.0  2020-11-01 00:08:23  ...       1.0                 2.75
1       2.0  2020-11-01 00:23:32  ...       1.0                 0.00

[2 rows x 20 columns]
15:52:26.387 | INFO    | Task run 'clean-0' - columns: VendorID                        float64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                      float64
PULocationID                      int64
DOLocationID                      int64
passenger_count                 float64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                    float64
trip_type                       float64
congestion_surcharge            float64
dtype: object
15:52:26.389 | INFO    | Task run 'clean-0' - rows: 88605
15:52:26.420 | INFO    | Task run 'clean-0' - Finished in state Completed()
15:52:26.455 | INFO    | Flow run 'vivacious-alligator' - Created task run 'write_local-0' for task 'write_local'
15:52:26.456 | INFO    | Flow run 'vivacious-alligator' - Executing 'write_local-0' immediately...
15:52:26.829 | INFO    | Task run 'write_local-0' - Finished in state Completed()
15:52:26.865 | INFO    | Flow run 'vivacious-alligator' - Created task run 'write_gcs-0' for task 'write_gcs'
15:52:26.866 | INFO    | Flow run 'vivacious-alligator' - Executing 'write_gcs-0' immediately...
15:52:26.999 | INFO    | Task run 'write_gcs-0' - Getting bucket 'prefect-de-zoomcamp-jpvr-2023'.
15:52:27.141 | INFO    | Task run 'write_gcs-0' - Uploading from PosixPath('data/green/green_tripdata_2020-11.parquet') to the bucket 'prefect-de-zoomcamp-jpvr-2023' path 'data/green/green_tripdata_2020-11.parquet'.
15:52:27.459 | INFO    | Task run 'write_gcs-0' - Finished in state Completed()
15:52:27.460 | INFO    | Flow run 'vivacious-alligator' - 

Rows processed: 88605

15:52:27.499 | INFO    | Flow run 'vivacious-alligator' - Finished in state Completed('All states completed.')
15:52:28.084 | INFO    | prefect.infrastructure.process - Process 'vivacious-alligator' exited cleanly.
```

---
#### Q5
> It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur.

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up.

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook.

Join my temporary Slack workspace with this link. 400 people can use this link and it expires in 90 days.

In the Prefect Cloud UI create an Automation or in the Prefect Orion UI create a Notification to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create.

How many rows were processed by the script?

- [] 125,268
- [] 377,922
- [] 728,390
- [X] 514,392

> Answer

```bash
prefect deployment build ./week02/homework02/etl_web_to_gcs.py:etl_web_to_gcs 
--name q5_param_etl_web_to_gcs 
-sb github/test 
--params='{"month" : 4, "year": 2019, "color": "green"}' 
--apply
```

```log
Found flow 'etl-web-to-gcs'
Deployment YAML created at '/home/jp/github/prefect-zoomcamp/flows/03_deployments/etl_web_to_gcs-deployment.yaml'.
Deployment storage GitHub(repository='https://github.com/jp-chl/2023-de/', reference=None, access_token=None, 
include_git_objects=True) does not have upload capabilities; no files uploaded.  Pass --skip-upload to suppress this warning.
Deployment 'etl-web-to-gcs/q5_param_etl_web_to_gcs' successfully created with id '63cd50e3-e67b-4c7e-bfa9-a46ddc9f8bee'.

To execute flow runs from this deployment, start an agent that pulls work from the 'default' work queue:
$ prefect agent start -q 'default'
```

```log
18:57:52.061 | INFO    | prefect.agent - Submitting flow run '22ccd97c-1662-438f-bb9d-8610681d4b00'
18:57:52.151 | INFO    | prefect.infrastructure.process - Opening process 'powerful-pug'...
18:57:52.186 | INFO    | prefect.agent - Completed submission of flow run '22ccd97c-1662-438f-bb9d-8610681d4b00'
/home/jp/anaconda3/envs/prefect-dev/lib/python3.9/runpy.py:127: RuntimeWarning: 'prefect.engine' found in sys.modules after import of package 'prefect', but prior to execution of 'prefect.engine'; this may result in unpredictable behaviour
  warn(RuntimeWarning(msg))
18:57:55.092 | INFO    | Flow run 'powerful-pug' - Downloading flow code from storage at ''
18:57:55.728 | INFO    | Flow run 'powerful-pug' - dataset_url: [https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-04.csv.gz]
18:57:55.766 | INFO    | Flow run 'powerful-pug' - Created task run 'fetch-0' for task 'fetch'
18:57:55.768 | INFO    | Flow run 'powerful-pug' - Executing 'fetch-0' immediately...
18:57:57.850 | INFO    | Task run 'fetch-0' - Finished in state Completed()
18:57:57.889 | INFO    | Flow run 'powerful-pug' - Created task run 'clean-0' for task 'clean'
18:57:57.891 | INFO    | Flow run 'powerful-pug' - Executing 'clean-0' immediately...
18:57:58.212 | INFO    | Task run 'clean-0' -    VendorID lpep_pickup_datetime  ... trip_type congestion_surcharge
0         2  2019-04-01 00:18:40  ...         1                 2.75
1         2  2019-04-01 00:18:24  ...         1                 0.00

[2 rows x 20 columns]
18:57:58.215 | INFO    | Task run 'clean-0' - columns: VendorID                          int64
lpep_pickup_datetime     datetime64[ns]
lpep_dropoff_datetime    datetime64[ns]
store_and_fwd_flag               object
RatecodeID                        int64
PULocationID                      int64
DOLocationID                      int64
passenger_count                   int64
trip_distance                   float64
fare_amount                     float64
extra                           float64
mta_tax                         float64
tip_amount                      float64
tolls_amount                    float64
ehail_fee                       float64
improvement_surcharge           float64
total_amount                    float64
payment_type                      int64
trip_type                         int64
congestion_surcharge            float64
dtype: object
18:57:58.216 | INFO    | Task run 'clean-0' - rows: 514392
18:57:58.253 | INFO    | Task run 'clean-0' - Finished in state Completed()
18:57:58.293 | INFO    | Flow run 'powerful-pug' - Created task run 'write_local-0' for task 'write_local'
18:57:58.294 | INFO    | Flow run 'powerful-pug' - Executing 'write_local-0' immediately...
18:58:00.276 | INFO    | Task run 'write_local-0' - Finished in state Completed()
18:58:00.317 | INFO    | Flow run 'powerful-pug' - Created task run 'write_gcs-0' for task 'write_gcs'
18:58:00.319 | INFO    | Flow run 'powerful-pug' - Executing 'write_gcs-0' immediately...
18:58:00.458 | INFO    | Task run 'write_gcs-0' - Getting bucket 'prefect-de-zoomcamp-jpvr-2023'.
18:58:00.636 | INFO    | Task run 'write_gcs-0' - Uploading from PosixPath('data/green/green_tripdata_2019-04.parquet') to the bucket 'prefect-de-zoomcamp-jpvr-2023' path 'data/green/green_tripdata_2019-04.parquet'.
18:58:01.004 | INFO    | Task run 'write_gcs-0' - Finished in state Completed()
18:58:01.006 | INFO    | Flow run 'powerful-pug' - 

Rows processed: 514392

18:58:01.045 | INFO    | Flow run 'powerful-pug' - Finished in state Completed('All states completed.')
18:58:01.685 | INFO    | prefect.infrastructure.process - Process 'powerful-pug' exited cleanly.
```

> Adding Webhook in "temp-notify.slack.com" Slack workspace

<p align="center">
  <img src="readme-images/05-a-add-integration.png" width="60%">
</p>

<p align="center">
  <img src="readme-images/05-b-added-webhook.png" width="60%">
</p>

<p align="left">
  <img src="readme-images/05-d-slack-add-integration.png" width="40%">
</p>

> Create notification in Prefect

<p align="center">
  <img src="readme-images/05-c-prefect-add-notification.png" width="70%">
</p>

> Run the deployment and post messages to Slack

<p align="left">
  <img src="readme-images/05-e-flow-run.png" width="70%">
</p>


Slack message: https://temp-notify.slack.com/archives/C04M4NAM67L/p1675883403549799

---

#### Q6
> Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service.

Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- [] 5
- [] 6
- [X] 8
- [] 10

> Answer

Create a secret block

<p align="left">
  <img src="readme-images/06-a.png" width="70%">
</p>

Add the 10 digit password

<p align="left">
  <img src="readme-images/06-b.png" width="70%">
</p>

Validate created secret block. 8 asterisk characters are shown in the UI

<p align="left">
  <img src="readme-images/06-c.png" width="70%">
</p>