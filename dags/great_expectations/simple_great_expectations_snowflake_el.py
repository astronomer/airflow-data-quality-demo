import os
import json

from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator
from include.great_expectations.configs.snowflake_configs import (
    snowflake_data_context_config,
    snowflake_checkpoint_config,
    snowflake_batch_request
)

# This table variable is a placeholder, in a live environment, it is better
# to pull the table info from a Variable in a template
TABLE = "YELLOW_TRIPDATA"
DATES = ["2019-01", "2019-02"]
TASK_DICT = {}
SNOWFLAKE_CONN_ID = "snowflake_default"

base_path = Path(__file__).parents[2]
expectation_file = os.path.join(
    base_path, "include", "great_expectations/expectations/taxi/demo.json"
)
data_file = os.path.join(
    base_path, "include", "data/yellow_tripdata_sample_2019-01.csv"
)
data_dir = os.path.join(base_path, "include", "data")
ge_root_dir = os.path.join(base_path, "include", "great_expectations")

data_context_config = snowflake_data_context_config
checkpoint_config = snowflake_checkpoint_config
passing_batch_request = snowflake_batch_request

with DAG(
    "great_expectations_snowflake_example",
    start_date=datetime(2021, 1, 1),
    description="Example DAG showcasing loading and data quality checking with Snowflake and Great Expectations.",
    schedule_interval=None,
    default_args={
        # TODO: update connection get here
        "snowflake_conn_id": SNOWFLAKE_CONN_ID,
        "warehouse": json.loads(BaseHook.get_connection(SNOWFLAKE_CONN_ID).extra)['extra__snowflake__warehouse'],
        "database": json.loads(BaseHook.get_connection(SNOWFLAKE_CONN_ID).extra)['extra__snowflake__database'],
        "role": json.loads(BaseHook.get_connection(SNOWFLAKE_CONN_ID).extra)['extra__snowflake__role'],
        "schema": BaseHook.get_connection(SNOWFLAKE_CONN_ID).schema,
    },
    template_searchpath=f"{base_path}/include/sql/great_expectations_examples/",
    catchup=False,
) as dag:
    """
    ### Simple EL Pipeline with Data Quality Checks Using Snowflake and Great Expectations

    Ensure a Snowflake Warehouse, Database, Schema, Role, and S3 Key and Secret
    exist for the Snowflake connection, named `snowflake_default`. Access to S3
    is needed for this example. A staging table may need to be created in
    Snowflake manually.

    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2. No transformations or business logic.
    3. Exact values of data to quality check are known.

    """

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    """
    #### Snowflake table creation
    Create the table to store sample data.
    """
    create_snowflake_table = SnowflakeOperator(
        task_id="create_snowflake_table",
        sql="{% include 'create_snowflake_yellow_tripdata_table.sql' %}",
        params={"table_name": TABLE,
                "schema": BaseHook.get_connection(SNOWFLAKE_CONN_ID).schema}
    )

    """
    #### Delete table
    Clean up the table created for the example.
    """
    delete_snowflake_table = SnowflakeOperator(
        task_id="delete_snowflake_table",
        sql="{% include 'delete_snowflake_table.sql' %}",
        params={"table_name": TABLE},
        trigger_rule="all_done"
    )

    """
    #### Snowflake load task
    Loads the S3 data from the previous load to a Redshift table (specified
    in the Airflow Variables backend).
    """
    load_to_snowflake = S3ToSnowflakeOperator(
        task_id="load_to_snowflake",
        prefix="test/tripdata",
        stage=f"{TABLE}_STAGE",
        table=TABLE,
        file_format="(type = 'CSV', skip_header = 1, time_format = 'YYYY-MM-DD HH24:MI:SS')"
    )

    for i, date in enumerate(DATES):
        file_name = f"yellow_tripdata_sample_{date}.csv"
        file_path = f"{data_dir}/{file_name}"

        """
        #### Upload task
        Simply loads the file to a specified location in S3.
        """
        TASK_DICT[f"upload_to_s3_{date}"] = LocalFilesystemToS3Operator(
            task_id=f"upload_to_s3_{date}",
            filename=file_path,
            dest_key="{{ var.json.aws_configs.s3_key_prefix }}/tripdata/" + file_name,
            dest_bucket="{{ var.json.aws_configs.s3_bucket }}",
            aws_conn_id="aws_default",
            replace=True
        )

    """
    #### Great Expectations suite
    Run the Great Expectations suite on the table.
    """
    ge_snowflake_validation = GreatExpectationsOperator(
        task_id="ge_snowflake_validation",
        data_context_config=data_context_config,
        checkpoint_config=checkpoint_config
    )

    chain(
            begin,
            create_snowflake_table,
            load_to_snowflake,
            ge_snowflake_validation,
            delete_snowflake_table,
            end
        )
