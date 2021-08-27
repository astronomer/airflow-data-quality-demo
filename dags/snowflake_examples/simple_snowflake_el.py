from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator, SnowflakeCheckOperator, SnowflakeValueCheckOperator
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup

import json


SNOWFLAKE_SCHEMA = 'SANDBOX_BENJI'
SNOWFLAKE_WAREHOUSE = 'DEMO'
SNOWFLAKE_DATABASE = 'DWH_LEGACY'
SNOWFLAKE_ROLE = 'BENJI'
SNOWFLAKE_FORESTFIRE_TABLE = 'forestfires'

with DAG('simple_snowflake_el',
         default_args=default_args,
         description='Example DAG showcasing loading and data quality checking with Snowflake.',
         start_date=datetime(2021, 1, 1),
         schedule_interval=None,
         catchup=False) as dag:
    """
    ### Simple EL Pipeline with Data Quality Checks Using Snowflake

    Before running the DAG, set the following in an Airflow or Environment Variable:
    - key:

    Ensure a Snowflake Warehouse, Database, Schema, and Role exist for the Snowflake
    connection provided to the Operator. The names of these data should replace the
    dummy values at the top of the file.

    A Snowflake Connection is also needed, named `snowflake_default`.

    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2. No transformations or business logic.
    3. Exact values of data to quality check are known.
    """

    """
    #### Snowflake table creation
    Create the table to store sample forest fire data.
    """
    create_table = SnowflakeOperator(
        task_id="create_table",
        sql="{% include 'sql/create_snowflake_table.sql' %}",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE}
    )

    """
    #### Insert data
    Insert data into the Snowflake table using an existing SQL query (stored in
    the local sql/ directory).
    """
    load_data = SnowflakeOperator(
        task_id="insert_query",
        sql="{% include 'sql/load_snowflake_forestfire_data.sql' %}",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        params={"database_name": SNOWFLAKE_DATABASE, "table_name": SNOWFLAKE_FORESTFIRE_TABLE}
    )

    """
    #### Row-level data quality check
    Run data quality checks on a few rows, ensuring that the data in Snowflake
    matches the ground truth in the correspoding JSON file.
    """
    with open("/files/dags/snowflake_examples/forestfire_validation.json") as ffv:
        with TaskGroup(group_id="row_quality_checks") as quality_check_group:
            ffv_json = json.load(ffv)
            for id, values in ffv_json.items():
                values["id"] = id
                values["database_name"] = SNOWFLAKE_DATABASE
                values["table_name"] = SNOWFLAKE_FORESTFIRE_TABLE
                SnowflakeCheckOperator(
                    task_id=f"check_row_data_{id}",
                    sql="sql/row_quality_snowflake_forestfire_check.sql",
                    params=values
                )

    """
    #### Table-level data quality check
    Run a row count check to ensure all data was uploaded to Snowflake properly.
    """
    check_bq_row_count = SnowflakeValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM {SNOWFLAKE_FORESTFIRE_TABLE}",
        pass_value=9
    )

    """
    #### Delete table
    Clean up the table created for the example.
    """
    delete_table = SnowflakeOperator(
        task_id="delete_table",
        sql="{% include 'sql/delete_snowflake_table.sql' %}",
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE,
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE}
    )

    begin = DummyOperator(task_id='begin')
    end = DummyOperator(task_id='end')

    begin >> create_table >> load_data
    load_data >> quality_check_group >> delete_table
    load_data >> check_bq_row_count >> delete_table
    delete_table >> end
