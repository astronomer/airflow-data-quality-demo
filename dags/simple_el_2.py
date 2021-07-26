from airflow import DAG, AirflowException
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.sql import SQLCheckOperator

import hashlib
import logging

# The file(s) to upload shouldn't be hardcoded in a production setting, this is just for demo purposes.
CSV_FILE_NAME = "forestfires.csv"
CSV_FILE_PATH = f"include/sample_data/{CSV_FILE_NAME}"
CSV_CORRUPT_FILE_NAME = "forestfires_corrupt.csv"
CSV_CORRUPT_FILE_PATH = f"include/sample_data/{CSV_CORRUPT_FILE_NAME}"

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "astronomer",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 7),
    "email": ["noreply@astronomer.io"],
    "email_on_failure": False
}

with DAG("simple_el_dag_2",
         default_args=default_args,
         description="A sample Airflow DAG to load data from csv files to S3 and then Redshift, with data integrity checks.",
         schedule_interval=None,
         catchup=False) as dag:
    """
    ### Simple EL Pipeline with Data Integrity Check 2
    This is the second in a series of DAGs showing an EL pipeline with data integrity
    checking. A single file is uploaded to S3, then its ETag is verified
    against the MD5 hash of the local file. The two should match, which will
    allow the DAG to flow along the "happy path". To see the "sad path", change
    `csv_file_path` to `csv_corrupt_file_path` in the `validate_etag` task. If the
    "happy path" is continued, a second data load from S3 to Redshift is triggered,
    which is followed by another data integrity check. A similar "happy/sad path"
    branch ends the DAG.

    Before running the DAG, set the following in an Airflow or Environment Variable:
    - key: aws_configs
    - value: { "s3_bucket": [bucket_name], "s3_key_prefix": [key_prefix], "redshift_table": [table_name]}
    Fully replacing [bucket_name], [key_prefix], and [table_name].

    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2. No transformations or business logic.
    3. Single metric to validate (whether the uploads were successful).

    This demo solves the issue the simple_el_1 DAG left open: validating an
    upload to Redshift. However, it only validates that the data matches the
    source file; it does not guarantee that the source file's data is actually
    valid with respect to expectations about that data.
    """

    @task
    def upload_to_s3():
        """
        #### Upload task
        Simply loads the file to a specified location in S3.
        """
        aws_configs = Variable.get("aws_configs", deserialize_json=True)
        s3_bucket = aws_configs.get("s3_bucket")
        # Change `CSV_FILE_PATH` to `CSV_CORRUPT_FILE_PATH` for the "sad path".
        s3_key = aws_configs.get("s3_key_prefix") + "/" + CSV_FILE_PATH
        s3 = S3Hook()
        s3.load_file(CSV_FILE_PATH, s3_key, bucket_name=s3_bucket, replace=True)
        return { "s3_bucket": s3_bucket, "s3_key": s3_key }

    @task
    def validate_etag(s3_data):
        """
        #### Validation task
        Check the destination ETag against the local MD5 hash to ensure the file
        was uploaded without errors.
        """
        s3 = S3Hook()
        obj = s3.get_key(key=s3_data.get("s3_key"), bucket_name=s3_data.get("s3_bucket"))
        obj_etag = obj.e_tag.strip('"')
        file_hash = hashlib.md5(open(CSV_FILE_PATH).read().encode("utf-8")).hexdigest()
        if obj_etag != file_hash:
            raise AirflowException(f"Upload Error: Object ETag in S3 did not match hash of local file.")
        return s3_data

    upload_file = upload_to_s3()
    validate_file = validate_etag(upload_file)

    """
    #### Create Redshift Table (Optional)
    For demo purposes, create a Redshift table to store the forest fire data to.
    The database is not automatically destroyed at the end of the example; ensure
    this is done manually to avoid unnecessary costs. Additionally, set-up may
    need to be done in Airflow connections to allow access to Redshift. This step
    may also be accomplished manually; if it is, then the task should be removed below.
    """
    create_redshift_table = PostgresOperator(
        task_id='create_table',
        sql="sql/create_forestfire_table.sql",
        postgres_conn_id="redshift_default",
        params={"redshift_table": Variable.get("aws_configs", deserialize_json=True).get("redshift_table")}
    )

    """
    #### Second load task
    Loads the S3 data from the previous load to a Redshift table (specified
    in the Airflow Variables backend).
    """
    load_to_redshift = S3ToRedshiftOperator(
        s3_bucket=Variable.get("aws_configs", deserialize_json=True).get("s3_bucket"),
        s3_key=f'{Variable.get("aws_configs", deserialize_json=True).get("s3_key_prefix")}/{CSV_FILE_PATH}',
        schema="PUBLIC",
        table=Variable.get("aws_configs", deserialize_json=True).get("redshift_table"),
        copy_options=['csv'],
        task_id='load_to_redshift',
    )

    """
    #### Redshift row validation task
    Ensure that data was copied to Redshift from S3 correctly. A SQLCheckOperator is
    used here to check for any files in the stl_load_errors table.
    """
    validate_redshift = SQLCheckOperator(
        task_id="validate_redshift",
        conn_id="redshift_default",
        sql="sql/validate_forestfire_redshift_load.sql",
        params={"filename": CSV_FILE_NAME},
    )

    done = DummyOperator(task_id="done")

    validate_file >> create_redshift_table
    create_redshift_table >> load_to_redshift
    load_to_redshift >> validate_redshift
    validate_redshift >> done
