from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
# from prefect_gcp import GcpCredentials
from prefect_gcp import GcpCredentials

@task(retries=3, log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{color}_tripdata_{year}-{month:02}.parquet"
    print(f"gcs_path: [{gcs_path}]")
    gcs_block = GcsBucket.load("zoom-gcs")
    print(f"gcs_block: [{gcs_path}]")
    gd = gcs_block.get_directory(from_path=gcs_path, local_path=f"../")

    the_path = Path(f"../{gcs_path}")

    return the_path


@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    print(f"Skipping cleaning")
    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("zoom-gcs-creds")

    df.to_gbq(
        destination_table="dezoomcamp.rides",
        project_id="double-backup-374911",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow(log_prints=True)
def etl_gcs_to_bq(year: int, month: int, color: str) -> int:
    """Main ETL flow to load data into Big Query"""
    path = extract_from_gcs(color, year, month)
    
    df = transform(path)
    print(f"rows processed: [{str(len(df))}] for year {year}, month {month} and color {color}")

    write_bq(df)
    
    return len(df)

@flow(log_prints=True)
def etl_parent_flow(
    months: list[int] = [2, 3], year: int = 2019, color: str = "yellow"
):
    total_records_processed = 0

    for month in months:
        records_processed = etl_gcs_to_bq(year, month, color)
        total_records_processed += records_processed

    print(f"Total rows processed: [{total_records_processed}]")


if __name__ == "__main__":
    color = "yellow"
    months = [2, 3]
    year = 2019
    
    etl_parent_flow(months, year, color)