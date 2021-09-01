import os

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryDeleteDatasetOperator
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup
from great_expectations_provider.operators.great_expectations import GreatExpectationsBigQueryOperator


BASE_PATH = "/usr/local/airflow/"
EXPECTATION_FILE = os.path.join(
    BASE_PATH, "include", "great_expectations/expectations/taxi/demo.json"
)
DATA_FILE = os.path.join(
    BASE_PATH, "include", "data/yellow_tripdata_sample_2019-01.csv"
)

# In a production DAG, the global variables below should be stored as Airflow
# or Environment variables.
BQ_DATASET="great_expectations_bigquery_example"
BQ_TABLE="taxi"

GCP_BUCKET="great-expectations-demo"
GCP_DATA_DEST="data/yellow_tripdata_sample_2019-01.csv"
GCP_SUITE_DEST="expectations/taxi/demo.json"

with DAG("great_expectations_bigquery_example",
         description="Example DAG showcasing loading and data quality checking with BigQuery and Great Expectations.",
         schedule_interval=None,
         start_date=datetime(2021, 1, 1),
         catchup=False) as dag:
    """
    ### Simple EL Pipeline with Data Quality Checks Using BigQuery and Great Expectations

    Before running the DAG, set the following in an Airflow or Environment Variable:
    - key: gcp_project_id
      value: [gcp_project_id]
    Fully replacing [gcp_project_id] with the actual ID.

    Ensure you have a connection to GCP, using a role with access to BigQuery
    and the ability to create, modify, and delete datasets and tables.

    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2. No transformations or business logic.
    3. Exact values of data to quality check are known.
    """

    """
    #### BigQuery dataset creation
    Create the dataset to store the sample data tables.
    """
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=BQ_DATASET
    )

    """
    #### Upload taxi data to GCS
    Upload the test data to GCS so it can be transferred to BigQuery.
    """
    upload_taxi_data = LocalFilesystemToGCSOperator(
        task_id="upload_taxi_data",
        src=DATA_FILE,
        dst=GCP_DATA_DEST,
        bucket=GCP_BUCKET,
    )

    """
    #### Transfer data from GCS to BigQuery
    Moves the data uploaded to GCS in the previous step to BigQuery, where
    Great Expectations can run a test suite against it.
    """
    transfer_taxi_data = GCSToBigQueryOperator(
        task_id="taxi_data_gcs_to_bigquery",
        bucket=GCP_BUCKET,
        source_objects=[GCP_DATA_DEST],
        skip_leading_rows=1,
        destination_project_dataset_table="{}.{}".format(BQ_DATASET, BQ_TABLE),
        schema_fields=[
            {"name": "vendor_id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "pickup_datetime", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "dropoff_datetime", "type": "DATETIME", "mode": "NULLABLE"},
            {"name": "passenger_count", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "trip_distance", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rate_code_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
            {"name": "pickup_location_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "dropoff_location_id", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "payment_type", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "fare_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "extra", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "mta_tax", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tip_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "tolls_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "improvement_surcharge", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "total_amount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "congestion_surcharge", "type": "FLOAT", "mode": "NULLABLE"}
        ],
        source_format="CSV",
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE",
        allow_jagged_rows=True
    )

    """
    #### Upload test suite to GCS
    The GreatExpectationsBigQueryOperator expects the test suite to reside in
    GCS, so the local file gets uploaded to GCS here.
    """
    upload_expectations_suite = LocalFilesystemToGCSOperator(
        task_id="upload_test_suite",
        src=EXPECTATION_FILE,
        dst=GCP_SUITE_DEST,
        bucket=GCP_BUCKET,
    )

    """
    #### Great Expectations suite
    Run the Great Expectations suite on the table.
    """
    ge_bigquery_validation = GreatExpectationsBigQueryOperator(
        task_id="ge_bigquery_validation",
        gcp_project="{{ var.value.gcp_project_id }}",
        gcs_bucket=GCP_BUCKET,
        # GE will use a folder "$my_bucket/expectations"
        gcs_expectations_prefix="expectations",
        # GE will use a folder "$my_bucket/validations"
        gcs_validations_prefix="validations",
        # GE will use a folder "$my_bucket/data_docs"
        gcs_datadocs_prefix="data_docs",
        # GE will look for a file $my_bucket/expectations/taxi/demo.json
        expectation_suite_name="taxi.demo",
        table=BQ_TABLE,
        bq_dataset_name=BQ_DATASET,
        bigquery_conn_id="google_cloud_default"
    )

    """
    #### Delete test dataset and table
    Clean up the dataset and table created for the example.
    """
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        project_id="{{ var.value.gcp_project_id }}",
        dataset_id=BQ_DATASET,
        delete_contents=True
    )

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    chain(begin, create_dataset, upload_taxi_data, transfer_taxi_data,
          upload_expectations_suite, ge_bigquery_validation, delete_dataset, end)
