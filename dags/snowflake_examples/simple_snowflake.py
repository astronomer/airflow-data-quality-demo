"""
### Simple EL Pipeline with Data Quality Checks Using Snowflake

Runs a data quality check, in SQL, on the forest fires dataset

Note that this DAG will clean up after itself and delete all data it uploads.

Ensure a Snowflake Warehouse, Database, Schema, and Role exist for the Snowflake
connection provided to the Operator. The names of these data should replace the
dummy values at the top of the file.

A Snowflake Connection is also needed, named `snowflake_default`.

What makes this a simple data quality case is:
1. Absolute ground truth: the local CSV file is considered perfect and immutable.
2. No transformations or business logic.
3. Exact values of data to quality check are known.
"""

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup


SNOWFLAKE_FORESTFIRE_TABLE = "forestfires"
SNOWFLAKE_CONN_ID = "snowflake_default"


with DAG(
    "simple_snowflake",
    description="Example DAG showcasing loading and data quality checking with Snowflake.",
    doc_md=__doc__,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    template_searchpath="/usr/local/airflow/include/sql/snowflake_examples/",
    catchup=False
) as dag:


    """
    #### Snowflake table creation
    Create the table to store sample forest fire data.
    """
    create_table = SnowflakeOperator(
        task_id="create_table",
        sql="{% include 'create_forestfire_table.sql' %}",
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE}
    )

    """
    #### Insert data
    Insert data into the Snowflake table using an existing SQL query (stored in
    the include/sql/snowflake_examples/ directory).
    """
    load_data = SnowflakeOperator(
        task_id="insert_query",
        sql="{% include 'load_snowflake_forestfire_data.sql' %}",
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE}
    )

    with TaskGroup(group_id="quality_checks", default_args={"conn_id": SNOWFLAKE_CONN_ID}) as quality_check_group:
        """
        #### Column-level data quality check
        Run data quality checks on columns of the audit table
        """
        column_checks = SQLColumnCheckOperator(
            task_id="column_checks",
            table=SNOWFLAKE_FORESTFIRE_TABLE,
            column_mapping={"ID": {"null_check": {"equal_to": 0}}}
        )

        """
        #### Table-level data quality check
        Run data quality checks on the audit table
        """
        table_checks = SQLTableCheckOperator(
            task_id="table_checks",
            table=SNOWFLAKE_FORESTFIRE_TABLE,
            checks={"row_count_check": {"check_statement": "COUNT(*) = 9"}}
        )

    """
    #### Delete table
    Clean up the table created for the example.
    """
    delete_table = SnowflakeOperator(
        task_id="delete_table",
        sql="{% include 'delete_snowflake_table.sql' %}",
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE}
    )

    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

    chain(
        begin,
        create_table,
        load_data,
        quality_check_group,
        delete_table,
        end
    )
