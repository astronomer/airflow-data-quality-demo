from airflow import DAG, AirflowException
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.sql import SqlSensor
from airflow.operators.sql import SQLCheckOperator
from airflow.utils.task_group import TaskGroup

import hashlib
import json
import logging

# The file(s) to upload shouldn't be hardcoded in a production setting, this is just for demo purposes.
CSV_FILE_NAME = "forestfires.csv"
CSV_FILE_PATH = f"include/sample_data/{CSV_FILE_NAME}"
CSV_CORRUPT_FILE_NAME = "forestfires_corrupt.csv"
CSV_CORRUPT_FILE_PATH = f"include/sample_data/{CSV_CORRUPT_FILE_NAME}"
CSV_INVALID_FILE_NAME = "forestfires_invalid.csv"
CSV_INVALID_FILE_PATH = f"include/sample_data/{CSV_INVALID_FILE_NAME}"
AWS_CONFIGS = Variable.get("aws_configs", deserialize_json=True)
S3_BUCKET = AWS_CONFIGS.get("s3_bucket")
S3_KEY = AWS_CONFIGS.get("s3_key_prefix") + "/" + CSV_FILE_PATH
REDSHIFT_TABLE = AWS_CONFIGS.get("redshift_table")

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "astronomer",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 7),
    "email": ["noreply@astronomer.io"],
    "email_on_failure": False
}

with DAG("simple_el_dag_3",
         default_args=default_args,
         description="A sample Airflow DAG to load data from csv files to S3 and then Redshift, with data integrity and quality checks.",
         schedule_interval=None,
         catchup=False) as dag:
    """
    ### Simple EL Pipeline with Data Integrity and Quality Checks 3
    This is the third in a series of DAGs showing an EL pipeline with data integrity
    and data quality checking. A single file is uploaded to S3, then its ETag is
    verified against the MD5 hash of the local file. The two should match, which
    will allow the DAG to continue to the next task. To see the DAG failure case,
    change `CSV_FILE_PATH` to `CSV_CORRUPT_FILE_PATH` in the `upload_to_s3` task.
    The failure here is caused by missing rows. If the "happy path" is continued,
    a second data load from S3 to Redshift is triggered, which is followed by
    another data integrity check. If the check fails, an Airflow Exception is raised.
    Otherwise, a final data quality check is performed on the Redshift table for
    the ID column; if this check fails, an Airflow Exception is raised.

    Before running the DAG, set the following in a Variable:
    - key: aws_configs
    - value: { "s3_bucket": [bucket_name], "s3_key_prefix": [key_prefix], "redshift_table": [table_name]}
    Fully replacing [bucket_name], [key_prefix], and [table_name].

    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2.
    3. Single metric to validate.
    """

    @task
    def upload_to_s3():
        """
        #### Upload task
        Simply loads the file to a specified location in S3.
        """
        s3 = S3Hook()
        s3.load_file(CSV_FILE_PATH, S3_KEY, bucket_name=S3_BUCKET, replace=True)

    @task
    def validate_etag():
        """
        #### Validation task
        Check the destination ETag against the local MD5 hash to ensure the file
        was uploaded without errors.
        """
        s3 = S3Hook()
        obj = s3.get_key(key=S3_KEY, bucket_name=S3_BUCKET)
        obj_etag = obj.e_tag.strip('"')
        file_hash = hashlib.md5(open(CSV_FILE_PATH).read().encode("utf-8")).hexdigest()
        if obj_etag != file_hash:
            raise AirflowException(f"Upload Error: Object ETag in S3 did not match hash of local file.")

    upload_files = upload_to_s3()
    validate_s3_file = validate_etag()

    """
    #### Create Redshift Table (Optional)
    For demo purposes, create a Redshift table to store the forest fire data to.
    The database is not automatically destroyed at the end of the example; ensure
    this is done manually to avoid unnecessary costs. Additionally, set-up may
    need to be done in Airflow connections to allow access to Redshift. This step
    may also be accomplished manually; it is, then the task should be removed below.
    """
    create_redshift_table = PostgresOperator(
        task_id='create_table',
        sql="sql/create_forestfire_table.sql",
        postgres_conn_id="redshift_default",
        params={"redshift_table": REDSHIFT_TABLE}
    )

    """
    #### Second load task
    Loads the S3 data from the previous load to a Redshift table (specified
    in the Airflow Variables backend).
    """
    load_to_redshift = S3ToRedshiftOperator(
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        schema="PUBLIC",
        table=REDSHIFT_TABLE,
        copy_options=['csv'],
        task_id='load_to_redshift'
    )

    def redshift_load_error(cell):
        # If there is a load error, log the query here so it can be checked
        # directly in Redshift for further details.
        logging.info(f"Checking failure case for cell: {cell}")
        return False #Change back to True!

    """
    #### Redshift row validation task
    Ensure that data was copied to Redshift from S3 correctly. A SQL Sensor is
    used here to check for any files in the stl_load_errors table. If the failure
    callback (above) is triggered, the query number is printed to the task log.
    """
    validate_redshift = SqlSensor(
        task_id="validate_redshift",
        conn_id="redshift_default",
        sql="sql/validate_forestfire_redshift_load.sql",
        params={"filename": CSV_FILE_NAME},
        failure=redshift_load_error
    )

    """
    #### Row-level data quality check
    Run a data quality check on a few rows, ensuring that the data in Redshift
    matches the ground truth in the correspoding JSON file.
    """
    with open("include/validation/forestfire_validation.json") as ffv:
        with TaskGroup(group_id="row_quality_checks") as quality_check_group:
            ffv_json = json.load(ffv)
            for id, values in ffv_json.items():
                values["id"] = id
                values["redshift_table"] = REDSHIFT_TABLE
                SQLCheckOperator(
                    task_id=f"forestfire_row_quality_check_{id}",
                    conn_id="redshift_default",
                    sql="sql/forestfire_row_quality_check.sql",
                    params=values,
                )

    done = DummyOperator(task_id="done")

    upload_files >> validate_s3_file
    validate_s3_file >> create_redshift_table
    create_redshift_table >> load_to_redshift
    load_to_redshift >> validate_redshift
    validate_redshift >> quality_check_group
    quality_check_group >> done
