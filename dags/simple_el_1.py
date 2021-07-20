from airflow import DAG, AirflowException
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import datetime
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import hashlib

# The file(s) to upload shouldn't be hardcoded in a production setting, this is just for demo purposes.
CSV_FILE_NAME = "forestfires.csv"
CSV_FILE_PATH = f"include/sample_data/{CSV_FILE_NAME}"
CSV_CORRUPT_FILE_NAME = "forestfires_corrupt.csv"
CSV_CORRUPT_FILE_PATH = f"include/sample_data/{CSV_CORRUPT_FILE_NAME}"
AWS_CONFIGS = Variable.get("aws_configs", deserialize_json=True)
S3_BUCKET = AWS_CONFIGS.get("s3_bucket")
S3_KEY = AWS_CONFIGS.get("s3_key_prefix") + "/" + CSV_FILE_PATH

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "astronomer",
    "depends_on_past": False,
    "start_date": datetime(2021, 7, 7),
    "email": ["noreply@astronomer.io"],
    "email_on_failure": False
}

with DAG("simple_el_dag_1",
         default_args=default_args,
         description="A sample Airflow DAG to load data from csv files to S3, then check that all data was uploaded properly.",
         schedule_interval=None,
         catchup=False) as dag:
    """
    ### Simple EL Pipeline with Data Integrity Check 1
    This is a very simple DAG showing a minimal EL data pipeline with a data
    integrity check. A single file is uploaded to S3, then its ETag is verified
    against the MD5 hash of the local file. The two should match, which will
    allow the DAG to flow along the "happy path". To see the "sad path", change
    `csv_file_path` to `csv_corrupt_file_path` in the `validate_etag` task.

    Before running the DAG, set the following in a Variable:
    - key: aws_configs
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
        # Change `csv_file_path` to `csv_corrupt_file_path` for the "sad path".
        file_hash = hashlib.md5(open(CSV_FILE_PATH).read().encode("utf-8")).hexdigest()
        if obj_etag != file_hash:
            raise AirflowException(f"Upload Error: Object ETag in S3 did not match hash of local file.")

    upload_files = upload_to_s3()
    validate_file = validate_etag()

    done = DummyOperator(task_id="done")

    upload_files >> validate_file
    validate_file >> done
