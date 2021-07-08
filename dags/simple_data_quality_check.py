from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import datetime
from airflow.models import Variable

import boto3
import hashlib

# The file(s) to upload shouldn't be hardcoded in a production setting, this is just for demo purposes.
file_name = "sample_data/forestfires.csv"
dag_configs = Variable.get('s3_simple_dq',
                            deserialize_json=True)
s3_bucket = dag_configs.get('s3_bucket')
s3_key = dag_configs.get('s3_key_prefix') + '/' + file_name
s3 = boto3.client('s3')

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
         description="A sample Airflow DAG to load data from csv files to Redshift, then check that all data was uploaded properly.",
         schedule_interval=None,
         catchup=False) as dag:

    def upload_to_s3():
        with open(file_name, "rb") as f:
            s3.upload_fileobj(f, s3_bucket, s3_key)

    # This task uploads data from the csv files to S3.
    upload_files = PythonOperator(
        task_id="upload_files",
        python_callable=upload_to_s3
    )

    def validate_etag():
        obj_meta = s3.head_object(Bucket=s3_bucket, Key=s3_key)
        obj_etag = obj_meta['ETag'].strip('"')
        file_hash = hashlib.md5(open(file_name).read()).hexdigest()
        if obj_etag == file_hash:
            return 'valid'
        return 'invalid'

    validate_files = BranchPythonOperator(
        task_id="validate_files",
        python_callable=validate_etag
    )

    valid = DummyOperator(task_id='valid')

    invalid = EmailOperator(
        task_id='invalid',
        to='',
        subject='Simple Data Quality Check Failed',
        html_content='Hello, your data quality check has failed.'
    )

    upload_files >> validate_files
    validate_files >> valid
    validate_files >> invalid
