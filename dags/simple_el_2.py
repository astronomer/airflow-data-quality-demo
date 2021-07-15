from airflow import DAG
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import datetime
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

import hashlib
import logging

# The file(s) to upload shouldn't be hardcoded in a production setting, this is just for demo purposes.
CSV_FILE_NAME = "forestfires.csv"
CSV_FILE_PATH = f"sample_data/{CSV_FILE_NAME}"
CSV_CORRUPT_FILE_PATH = "sample_data/forestfires_corrupt.csv"
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

with DAG("simple_el_dag_2",
         default_args=default_args,
         description="A sample Airflow DAG to load data from csv files to S3 and then Redshift, with data integrity checks.",
         schedule_interval=None,
         catchup=False) as dag:
    """
    ### Simple EL Pipeline with Data Integrity Check 2
    This is the second in a series of DAGs showing an EL pipeline with data integrity
    checking. A single file is uploaded to S3, then it's ETag is verified
    against the MD5 hash of the local file. The two should match, which will
    allow the DAG to flow along the "happy path". To see the "sad path", change
    `csv_file_path` to `csv_corrupt_file_path` in the `validate_etag` task. If the
    "happy path" is continued, a second data load from S3 to Redshift is triggered,
    which is followed by another data integrity check. A similar "happy/sad path"
    branch ends the DAG.

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
        # Change `CSV_FILE_PATH` to `CSV_CORRUPT_FILE_PATH` for the "sad path".
        file_hash = hashlib.md5(open(CSV_FILE_PATH).read().encode("utf-8")).hexdigest()
        if obj_etag == file_hash:
            return "load_to_redshift"
        return "s3_load_fail"

    upload_files = upload_to_s3()
    validate_s3_file = validate_etag()

    s3_load_fail = EmailOperator(
        task_id="s3_load_fail",
        to="",
        subject="Simple EL DAG: S3 Upload Failed",
        html_content=f"The data integrity check for {CSV_FILE_NAME} has failed during upload to S3."
    )

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
        task_id='load_to_redshift',
    )

    @task
    def validate_redshift_table():
        """
        #### Redshift row validation task
        Ensure that data was copied to Redshift from S3 correctly.
        """
        get_stl_load_commit = """SELECT query, trim(filename) as filename, curtime, status
                                 FROM stl_load_commits
                                 WHERE filename LIKE '%{filename}%' ORDER BY query;
                                 """.format(filename=CSV_FILE_NAME)
        pg_hook = PostgresHook(postgres_conn_id="redshift_default")
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(get_stl_load_commit)
        stl_load_commits = cursor.fetchall()
        logging.info(f"stl load commits: {stl_load_commits}")
        stl_load_commits.sort(key=lambda x: x[0], reverse=True)
        logging.info(f"last upload: {stl_load_commits[0]}")
        return "valid" if stl_load_commits[0][3] == 1 else "redshift_load_fail"

    validate_redshift = validate_redshift_table()

    redshift_load_fail = EmailOperator(
        task_id="redshift_load_fail",
        to="",
        subject="Simple EL DAG: Redshift Load Failed",
        html_content=f"The data integrity check for {CSV_FILE_NAME} has failed during load to Redshift."
    )

    valid = DummyOperator(task_id="valid")

    upload_files >> validate_s3_file
    validate_s3_file >> s3_load_fail
    validate_s3_file >> create_redshift_table
    create_redshift_table >> load_to_redshift
    load_to_redshift >> validate_redshift
    validate_redshift >> redshift_load_fail
    validate_redshift >> valid
