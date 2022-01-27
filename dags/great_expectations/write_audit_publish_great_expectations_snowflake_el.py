import os
import json

from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import (
    LocalFilesystemToS3Operator,
)
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from include.great_expectations.configs.snowflake_configs import (
    snowflake_data_context_config,
    snowflake_checkpoint_config,
)
from include.libs.schema_reg.base_schema_transforms import snowflake_load_column_string


# This table variable is a placeholder, in a live environment, it is better
# to pull the table info from a Variable in a template
table = "YELLOW_TRIPDATA"
snowflake_conn = "snowflake_default"

base_path = Path(__file__).parents[2]
expectation_file = os.path.join(
    base_path, "include", "great_expectations/expectations/taxi/demo.json"
)
# To see the failure case, change data_date from "2019-01" to "2019-02"
data_date = "2019-01"  # "2019-02"
data_file = os.path.join(
    base_path, "include", f"data/yellow_tripdata_sample_{data_date}.csv"
)

table_schema_path = f"{base_path}/include/sql/great_expectations_examples/table_schemas/"

data_dir = os.path.join(base_path, "include", "data")
ge_root_dir = os.path.join(base_path, "include", "great_expectations")

#data_context_config = snowflake_data_context_config
checkpoint_config = snowflake_checkpoint_config

with DAG(
    "great_expectations_snowflake_write_audit_publish_example",
    start_date=datetime(2022, 1, 1),
    description="Example DAG showcasing a write-audit-publish data quality pattern with Snowflake and Great Expectations.",
    schedule_interval=None,
    default_args={
        "snowflake_conn": snowflake_conn,
        "warehouse": json.loads(BaseHook.get_connection(snowflake_conn).extra)[
            "extra__snowflake__warehouse"
        ],
        "database": json.loads(BaseHook.get_connection(snowflake_conn).extra)[
            "extra__snowflake__database"
        ],
        "role": json.loads(BaseHook.get_connection(snowflake_conn).extra)[
            "extra__snowflake__role"
        ],
        "schema": BaseHook.get_connection(snowflake_conn).schema,
    },
    template_searchpath=f"{base_path}/include/sql/great_expectations_examples/",
    catchup=False,
) as dag:
    """
    ### Write-Audit-Publish Pattern EL Pipeline with Data Quality Checks Using Snowflake and Great Expectations

    Ensure a Snowflake Warehouse, Database, Schema, Role, and S3 Key and Secret
    exist for the Snowflake connection, named `snowflake_default`. Access to S3
    is needed for this example. An 'aws_configs' variable is needed in Variables,
    see the Redshift Examples in the README section for more information.

    The write-audit-publish pattern writes data to a staging table, audits the
    data quality through quality checks, then publishes correct data to a
    production table. In this example incorrect data is discarded, and the DAG
    is failed on data quality check failure.

    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2. No transformations or business logic.
    3. Exact values of data to quality check are known.
    """

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    """
    #### Upload task
    Loads the files to a specified location in S3
    """
    upload_to_s3 = LocalFilesystemToS3Operator(
        task_id="upload_to_s3",
        filename=data_file,
        dest_key="{{ var.json.aws_configs.s3_key_prefix }}/tripdata/yellow_tripdata_sample.csv",
        dest_bucket="{{ var.json.aws_configs.s3_bucket }}",
        aws_conn_id="aws_default",
        replace=True,
    )

    """
    #### Snowflake table creation
    Creates the tables to store sample data
    """
    create_snowflake_audit_table = SnowflakeOperator(
        task_id="create_snowflake_audit_table",
        sql="{% include 'create_yellow_tripdata_snowflake_table.sql' %}",
        params={
            "table_name": f"{table}_AUDIT",
            "schema": BaseHook.get_connection(snowflake_conn).schema,
        },
    )

    create_snowflake_table = SnowflakeOperator(
        task_id="create_snowflake_table",
        sql="{% include 'create_yellow_tripdata_snowflake_table.sql' %}",
        params={
            "table_name": table,
            "schema": BaseHook.get_connection(snowflake_conn).schema,
        },
    )

    """
    #### Delete table
    Cleans up the tables created for the example
    """
    delete_snowflake_audit_table = SnowflakeOperator(
        task_id="delete_snowflake_audit_table",
        sql="{% include 'delete_yellow_tripdata_table.sql' %}",
        params={"table_name": f"{table}_AUDIT"},
        #trigger_rule="all_done",
    )

    delete_snowflake_table = SnowflakeOperator(
        task_id="delete_snowflake_table",
        sql="{% include 'delete_yellow_tripdata_table.sql' %}",
        params={"table_name": table},
        #trigger_rule="all_done",
    )

    """
    #### Snowflake load task
    Loads the S3 data from the previous load to a Snowflake table (specified
    in the Airflow Variables backend)
    """
    load_s3_to_snowflake = S3ToSnowflakeOperator(
        task_id="load_s3_to_snowflake",
        prefix="test/tripdata",
        stage=f"{table}_STAGE",
        table=f"{table}_AUDIT",
        file_format="(type = 'CSV', skip_header = 1, time_format = 'YYYY-MM-DD HH24:MI:SS')",
        trigger_rule="all_done"
    )

    """
    #### Great Expectations suite
    Runs the Great Expectations suite on the table
    """
    ge_snowflake_validation = GreatExpectationsOperator(
        task_id="ge_snowflake_validation",
        data_context_root_dir=ge_root_dir,
        checkpoint_config=checkpoint_config,
    )

    with open(
        f"{table_schema_path}/tripdata_schema.json",
        "r",
    ) as f:
        table_schema = json.load(f).get("yellow_tripdata")
        table_props = table_schema.get("properties")
        table_dimensions = table_schema.get("dimensions")
        table_metrics = table_schema.get("metrics")

        col_string = snowflake_load_column_string(table_props)

        """
        #### Snowflake audit to production task
        Loads the data from the audit table to the production table
        """
        copy_snowflake_audit_to_production_table = SnowflakeOperator(
            task_id="copy_snowflake_audit_to_production_table",
            sql="{% include 'copy_yellow_tripdata_snowflake_staging.sql' %}",
            params={
                "table_name": table,
                "audit_table_name": f"{table}_AUDIT",
                "schema": BaseHook.get_connection(snowflake_conn).schema,
                "table_schema": table_props,
                "col_string": col_string
            },
        )

    chain(
        begin,
        upload_to_s3,
        [create_snowflake_table, create_snowflake_audit_table],
        load_s3_to_snowflake,
        ge_snowflake_validation,
        copy_snowflake_audit_to_production_table,
        [delete_snowflake_table, delete_snowflake_audit_table],
        end,
    )
