"""
### Simple EL Pipeline with Data Quality Checks Using Snowflake and Great Expectations

A simple example of performing data quality checks in Snowflake using Great Expectations.

Ensure a Snowflake Warehouse, Database, Schema, Role, and S3 Key and Secret
exist for the Snowflake connection, named `snowflake_default`. Access to S3
is needed for this example. An 'aws_configs' variable is needed in Variables,
see the Redshift Examples in the README section for more information.

What makes this a simple data quality case is:
1. Absolute ground truth: the local CSV file is considered perfect and immutable.
2. No transformations or business logic.
3. Exact values of data to quality check are known.
"""

import os
import pandas as pd

from pathlib import Path
from datetime import datetime
from pydoc import doc

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)
from include.great_expectations.configs.snowflake_configs import (
    snowflake_checkpoint_config, snowflake_data_context_config
)

# This table variable is a placeholder, in a live environment, it is better
# to pull the table info from a Variable in a template
table = "YELLOW_TRIPDATA"
base_path = Path(__file__).parents[2]
data_file = os.path.join(
    base_path,
    "include",
    "sample_data/yellow_trip_data/yellow_tripdata_sample_2019-01.csv",
)
ge_root_dir = os.path.join(base_path, "include", "great_expectations")
#checkpoint_config = snowflake_checkpoint_config

SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG(
    "great_expectations.snowflake",
    start_date=datetime(2021, 1, 1),
    description="Example DAG showcasing loading and data quality checking with Snowflake and Great Expectations.",
    doc_md=__doc__,
    schedule_interval=None,
    template_searchpath="/usr/local/airflow/include/sql/snowflake_examples/",
    catchup=False,
) as dag:

    """
    #### Snowflake table creation
    Create the table to store sample forest fire data.
    """
    create_table = SnowflakeOperator(
        task_id="create_table",
        sql="{% include 'create_forestfire_table.sql' %}",
        params={"table_name": table}
    )

    """
    #### Insert data
    Insert data into the Snowflake table using an existing SQL query (stored in
    the include/sql/snowflake_examples/ directory).
    """
    load_data = SnowflakeOperator(
        task_id="insert_query",
        sql="{% include 'load_snowflake_forestfire_data.sql' %}",
        params={"table_name": table}
    )

    """
    #### Delete table
    Clean up the table created for the example.
    """
    delete_table = SnowflakeOperator(
        task_id="delete_table",
        sql="{% include 'delete_snowflake_table.sql' %}",
        params={"table_name": table}
    )

    """
    #### Great Expectations suite
    Run the Great Expectations suite on the table.
    """
    ge_snowflake_validation = GreatExpectationsOperator(
        task_id="ge_snowflake_validation",
        data_context_config=snowflake_data_context_config,
        conn_id=SNOWFLAKE_CONN_ID,
        expectation_suite_name="taxi.demo",
        data_asset_name=table,
        fail_task_on_validation_failure=False,
    )

    ge_snowflake_query_validation = GreatExpectationsOperator(
        task_id="ge_snowflake_query_validation",
        data_context_config=snowflake_data_context_config,
        conn_id=SNOWFLAKE_CONN_ID,
        query_to_validate="SELECT *",
        expectation_suite_name="taxi.demo",
        data_asset_name=table,
        fail_task_on_validation_failure=False
    )

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    chain(
        begin,
        create_table,
        load_data,
        [
            ge_snowflake_validation,
            ge_snowflake_query_validation,
        ],
        delete_table,
        end,
    )
