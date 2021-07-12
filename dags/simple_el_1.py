from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import datetime
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import hashlib

# The file(s) to upload shouldn't be hardcoded in a production setting, this is just for demo purposes.
csv_file_name = "forestfires.csv"
csv_file_path = f"sample_data/{csv_file_name}"
csv_corrupt_file_path = "sample_data/forestfires_corrupt.csv"
dag_configs = Variable.get("s3_simple_dq", deserialize_json=True)
s3_bucket = dag_configs.get("s3_bucket")
s3_key = dag_configs.get("s3_key_prefix") + "/" + csv_file_path

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "astronomer",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 7),
    "email": ["noreply@astronomer.io"],
    "email_on_failure": False
}

with DAG("simple_dq_check_dag",
         default_args=default_args,
         description="A sample Airflow DAG to load data from csv files to S3, then check that all data was uploaded properly.",
         schedule_interval=None,
         catchup=False) as dag:
    """
    ### Simple Data Quality Check
    This is a very simple DAG showing a minimal data pipeline with a data
    integrity check. A single file is uploaded to S3, then it's ETag is verified
    against the MD5 hash of the local file. The two should match, which will
    allow the DAG to flow along the "happy path". To see the "sad path", change
    `csv_file_path` to `csv_corrupt_file_path` in the `validate_etag` task.

    Before running the DAG, set the following in a Variable:
        - key: s3_simple_dq
        - value: { "s3_bucket": [bucket_name], "s3_key_prefix": [key_prefix]}
    Fully replacing [bucket_name] and [key_prefix].

    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2. Single-step data pipeline: no business logic to complicate things.
    3. Single metric to validate.

    This demo works well in the case of validating data that is read from S3, such
    as other data pipelines that will read from S3, or Athena. It would not be
    helpful for data that is read from Redshift, as there is another load step
    that should be validated separately.
    """

    @task
    def upload_to_s3():
        """
        #### Upload task
        Simply loads the file to a specified location in S3.
        """
        s3 = S3Hook()
        s3.load_file(csv_file_path, s3_key, bucket_name=s3_bucket, replace=True)

    @task
    def validate_etag():
        """
        #### Validation task
        Check the destination ETag against the local MD5 hash to ensure the file
        was uploaded without errors.
        """
        s3 = S3Hook()
        obj = s3.get_key(key=s3_key, bucket_name=s3_bucket)
        obj_etag = obj.e_tag.strip('"')
        # Change `csv_file_path` to `csv_corrupt_file_path` for the "sad path".
        file_hash = hashlib.md5(open(csv_file_path).read().encode("utf-8")).hexdigest()
        if obj_etag == file_hash:
            return "valid"
        return "invalid"

    upload_files = upload_to_s3()
    validate_file = validate_etag()

    valid = DummyOperator(task_id="valid")

    invalid = EmailOperator(
        task_id="invalid",
        to="",
        subject="Simple Data Quality Check Failed",
        html_content=f"The data integrity check for {csv_file_name} has failed."
    )

    upload_files >> validate_file
    validate_file >> valid
    validate_file >> invalid
