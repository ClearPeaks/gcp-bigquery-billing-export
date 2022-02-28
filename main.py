import logging
import os
import sys
from datetime import date, timedelta
from typing import Any

import pandas as pd
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

# Configure logger
logging_format = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(stream=sys.stdout, format=logging_format, level=logging.INFO)
logger = logging.getLogger()

# Instantiate Google Cloud clients
CS = storage.Client()
BQ = bigquery.Client()

# Set global vars
project_id = os.environ["PROJECT_ID"]  # core project that contains all the data in CS and BigQuery
data_bucket_name = os.environ['BUCKET_NAME']  # name of the bucket containing all the data in CS
main_table_name = os.environ['MAIN_TABLE_NAME']  # name of main table in each dataset
bigquery_projects = os.environ['BIQUERY_PROJECTS'].split(',')  # Comma separated values (project1,table2...)
billing_project_id = os.environ['BILLING_PROJECT_ID']  # Billing project id
billing_dataset_id = os.environ['BILLING_DATASET_ID']  # Billing BigQuery dataset id

# Set global date vars
date_tc = date.today()
date_last_day_previous_month = date_tc - timedelta(days=date_tc.day)
date_first_day_previous_month = date_last_day_previous_month - timedelta(days=date_last_day_previous_month.day-1)

str_last_month = date_first_day_previous_month.strftime('%Y%m')


def generate_bq_jobs_costs_per_project() -> pd.DataFrame:
    """BigQuery Jobs Costs table data in DataFrame"""
    logger.info("Generating BigQuery Jobs Costs per User report")
    query = f"""
    DECLARE gb_divisor INT64 DEFAULT 1024*1024*1024;
    DECLARE tb_divisor INT64 DEFAULT gb_divisor*1024;
    DECLARE cost_per_tb_in_dollar INT64 DEFAULT 5;
    DECLARE cost_factor FLOAT64 DEFAULT cost_per_tb_in_dollar / tb_divisor;

    SELECT project_id,
           user_email,
           month,
           count(*) as num_queries,
           ROUND(SUM(total_bytes_billed)/gb_divisor, 2) as bytes_processed_in_gb,
           ROUND(SUM(cost_in_dollar), 4) as cost_in_dollar
    FROM(
        SELECT start_time,
               project_id,
               FORMAT_DATE("%Y%m", date(start_time)) as month,
               user_email,
               total_bytes_billed,
               IF(cache_hit != true, ROUND(total_bytes_processed * cost_factor,4),0) as cost_in_dollar
        FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
        WHERE job_type = 'QUERY'
            AND state = 'DONE'
            AND start_time BETWEEN '{date_first_day_previous_month.strftime('%Y-%m-%d')}' AND '{date_last_day_previous_month.strftime('%Y-%m-%d')}')
    GROUP BY user_email, project_id, month;
    """
    df = BQ.query(query).result().to_dataframe()
    logger.info(df)
    return df


def generate_bq_jobs_costs_detail() -> pd.DataFrame:
    """BigQuery Jobs Costs Detail table data in DataFrame"""
    logger.info("Generating BigQuery Jobs Costs Detail report")
    query = f"""
    DECLARE gb_divisor INT64 DEFAULT 1024*1024*1024;
    DECLARE tb_divisor INT64 DEFAULT gb_divisor*1024;
    DECLARE cost_per_tb_in_dollar INT64 DEFAULT 5;
    DECLARE cost_factor FLOAT64 DEFAULT cost_per_tb_in_dollar / tb_divisor;

    SELECT project_id,
           FORMAT_DATE("%Y%m", date(start_time)) as month,
           start_time,
           user_email,
           job_id,
           ROUND(total_bytes_billed/gb_divisor, 2) as bytes_processed_in_gb,
           IF(cache_hit != true, ROUND(total_bytes_processed * cost_factor,4),0) as cost_in_dollar
    FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
    WHERE job_type = 'QUERY'
        AND state = 'DONE'
        AND start_time BETWEEN '{date_first_day_previous_month.strftime('%Y-%m-%d')}' AND '{date_last_day_previous_month.strftime('%Y-%m-%d')}'
    ORDER BY project_id, start_time, user_email
    """

    df = BQ.query(query).result().to_dataframe()
    logger.info(df)
    return df


def generate_storage_usage() -> pd.DataFrame:
    """Storage usage table data in DataFrame"""
    logger.info("Generating Storage Usage report")
    data = []
    for project in bigquery_projects:
        logger.info(f"Scanning {project} table...")

        # BigQuery
        main_table_id = project_id + '.' + project + '.' + main_table_name
        logger.info(f"Checking {main_table_id}...")
        table = BQ.get_table(main_table_id)
        bq_project_size = table.num_bytes
        logger.info(f"{project} table has {table.num_rows} rows, {bq_project_size} bytes")

        # Cloud Storage
        path = f'gs://{data_bucket_name}/{project}'
        logger.info(f"Checking {path}...")
        cs_project_size = 0
        blobs = CS.list_blobs(data_bucket_name, prefix=project)
        for blob in blobs:
            cs_project_size += blob.size

        logger.info(f"{project}:\n\tBQ Table size: {bq_project_size}\n\tCS Folder size: {cs_project_size}")
        data.append([project, str_last_month, cs_project_size, bq_project_size])

    df = pd.DataFrame(data, columns=['project', 'month', 'cloud_storage_bytes', 'bigquery_storage_bytes'])
    logger.info(df)

    return df


def can_insert_data(table_name: str) -> bool:
    """
    Idempotent checks to avoid duplicate insertions
    Return:
    - True: table doesn't exist or data from last month haven't been inserted yet.
    - False: table exists and already has data from last month.
    """
    table_id = billing_project_id + '.' + billing_dataset_id + '.' + table_name
    logger.info(f"Checking if data is already stored from month {str_last_month} in {table_id}")
    try:
        # Check if table exists
        logger.info(f"Checking if {table_id} exists...")
        BQ.get_table(table_id)  # Raises NotFound if doesn't exist
        logger.info(f"Table {table_id} exists.")

        # Check if last month data is already there
        query = f"SELECT 1 FROM `{table_id}` WHERE month = '{str_last_month}'"
        logger.info(f"Checking if data exists in {table_id} with query: {query}")
        query_job = BQ.query(query)
        result = query_job.result()

        # Check if there is any result
        if result.total_rows == 0:
            logger.info(f"OK! No data present from previous month ({str_last_month}).")
            return True
        else:
            logger.warn(f"KO! Data present from previous month ({str_last_month}).")
            return False
    except NotFound:
        # Table doesn't exist, so allow inserting
        logger.info(f"Table {table_id} does not exist.")
        return True


def store_df_in_bq(df: pd.DataFrame,
                   table_name: str) -> None:
    """Store DataFrame as BigQuery table"""
    logger.info(f"Storing dataframe into {table_name}...")
    df.to_gbq(
        destination_table=billing_dataset_id + "." + table_name,
        project_id=billing_project_id,
        if_exists="append"  # Create or append
    )
    logger.info(f"Data successfully stored in {table_name}!")


def main(event: Any, context: Any) -> None:
    """Main and landing function for Cloud Function"""
    # Key/value with table name and function reference
    reports = {
        'storage_usage': generate_storage_usage,
        'bq_jobs_costs_detail': generate_bq_jobs_costs_detail,
        'bq_jobs_costs_per_project': generate_bq_jobs_costs_per_project
    }

    for table_name, func_ref in reports.items():
        if can_insert_data(table_name):
            df = func_ref()  # Call function to generate dataframe
            store_df_in_bq(df, table_name)
        else:
            logger.warn(f"Attempt to insert duplicated data in {table_name}")


if __name__ == "__main__":
    # This is for running from Cloud Shell
    # Cloud Function directly calls main()
    main(None, None)
