import os
import datetime

from airflow import models
from airflow.providers.google.cloud.transfers.sheets_to_gcs import GoogleSheetsToGCSOperator
from airflow.utils.dates import days_ago

from airflow import models
from airflow.operators import bash_operator
from airflow.operators import python_operator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryUpdateDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

DAG_ID = 'googlesheets_to_GCS_bq_dag'
ENV_ID = 'dev'
PROJECT_ID = 'sapient-cycling-373506'
BQ_DATASET_ID = 'airflow_Googlesheet_dataset'
BQ_DATASET_NAME = f"gsheet_dataset_{DAG_ID}_{ENV_ID}"
BUCKET = os.environ.get("GCP_GCS_BUCKET", "gsheet_bucket")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1EitVxj1Kv1sMyYR6vQjW_ClEodHYn0BUT3bKJFqd-RE")
FILE_NAME = "order_table_Sheet1.csv"
FILE_URI = f"gs://{BUCKET}/{FILE_NAME}"
with models.DAG(
    "google_sheets_to_gcs",
    start_date=days_ago(1),
    schedule_interval=None,  # Override to match your needs
    tags=["gsheet"],
) as dag:
    # [START upload_sheet_to_gcs]
    upload_sheet_to_gcs = GoogleSheetsToGCSOperator(
        task_id="upload_sheet_to_gcs",
        destination_bucket=BUCKET,
        spreadsheet_id=SPREADSHEET_ID,
        gcp_conn_id='google_cloud_gsheets'
    )
    # [END upload_sheet_to_gcs]
	
# create BigQuery Dataset
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", 
        dataset_id=BQ_DATASET_NAME,
        exists_ok=True,
        gcp_conn_id='google_cloud_gsheets')

    # create BigQuery Empty Table
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=BQ_DATASET_NAME,
        table_id="gsheet_test_table", 
        exists_ok=True,
        gcp_conn_id='google_cloud_gsheets')

    # Load Data in BigQuery Empty table
    load_data_in_bq = GCSToBigQueryOperator(
        task_id='load_data_in_bq',
        bucket=BUCKET,
        source_objects=FILE_NAME,
        destination_project_dataset_table=f'{BQ_DATASET_NAME}.gsheet_test_table',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_APPEND',
        field_delimiter=',',
        gcp_conn_id='google_cloud_gsheets')

    # Likewise, the end task calls a Bash script.
    end = bash_operator.BashOperator(
        task_id='end',
        bash_command='echo DAG completed successfully.'
        )

    # Define the order in which the tasks complete by using the >> and <<
    # operators. In this example, hello_python executes before goodbye_bash.
    upload_sheet_to_gcs >> create_dataset >> create_table >> load_data_in_bq >> end