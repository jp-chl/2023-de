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
  <img src="readme-images/03-run.png" width="70%">
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
- [] 88,605
- [] 190,225

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
- [] 514,392

---

#### Q6
> Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service.

Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

- [] 5
- [] 6
- [] 8
- [] 10
