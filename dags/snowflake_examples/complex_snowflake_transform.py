"""
### Snowflake ELT Pipeline with Multiple Datasets and Data Qality Checks

Run data quality checks, in SQL, on multiple Snowflake tables.

This DAG uses the forestfires public dataset on ForestFires to run data quality checks on multiple tables in Snowflake.
In the event of a failure, a Slack notification will be fired off. In this example, data quality checks are 
run as taskgroups after the data is uploaded.

Note that this DAG deletes all data it uploaded after the DQ checks run. 

Ensure a Snowflake Warehouse, Database, Schema, and Role exist for the Snowflake
connection provided to the operator under the connection ID `snowflake_default`.
"""

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import (SQLColumnCheckOperator,
                                                        SQLTableCheckOperator)
from airflow.providers.slack.operators.slack_webhook import \
    SlackWebhookOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup

SNOWFLAKE_FORESTFIRE_TABLE = "forestfires"
SNOWFLAKE_COST_TABLE = "costs"
SNOWFLAKE_FORESTFIRE_COST_TABLE = "forestfire_costs"

SNOWFLAKE_CONN_ID = "snowflake_default"

ROW_COUNT_CHECK = "COUNT(*) = 9"


def slack_failure_notification(context):
    task_id = context.get("task_instance").task_id
    dag_id = context.get("task_instance").dag_id
    exec_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    slack_msg = f"""
            :red_circle: Task Failed. 
            *Task*: {task_id}  
            *Dag*: {dag_id} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """
    failed_alert = SlackWebhookOperator(
        task_id="slack_notification",
        http_conn_id="slack_webhook",
        message=slack_msg,
        channel="data_engineering",
        username="failbot",
    )
    return failed_alert.execute(context=context)


with DAG(
    "complex_snowflake_transform",
    description="Example DAG showcasing loading, transforming, and data quality checking with multiple datasets in Snowflake.",
    doc_md=__doc__,
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    template_searchpath="/usr/local/airflow/include/sql/snowflake_examples/",
    catchup=False,
) as dag:
    """
    #### Snowflake table creation
    Create the tables to store sample data.
    """
    create_forestfire_table = SnowflakeOperator(
        task_id="create_forestfire_table",
        sql="create_forestfire_table.sql",
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE},
    )

    create_cost_table = SnowflakeOperator(
        task_id="create_cost_table",
        sql="create_cost_table.sql",
        params={"table_name": SNOWFLAKE_COST_TABLE},
    )

    create_forestfire_cost_table = SnowflakeOperator(
        task_id="create_forestfire_cost_table",
        sql="create_forestfire_cost_table.sql",
        params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    )

    """
    #### Insert data
    Insert data into the Snowflake tables using existing SQL queries
    stored in the include/sql/snowflake_examples/ directory.
    """
    load_forestfire_data = SnowflakeOperator(
        task_id="load_forestfire_data",
        sql="load_snowflake_forestfire_data.sql",
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE},
    )

    load_cost_data = SnowflakeOperator(
        task_id="load_cost_data",
        sql="load_cost_data.sql",
        params={"table_name": SNOWFLAKE_COST_TABLE},
    )

    load_forestfire_cost_data = SnowflakeOperator(
        task_id="load_forestfire_cost_data",
        sql="load_forestfire_cost_data.sql",
        params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    )

    """
    #### Transform
    Transform the forestfire_costs table to perform
    sample logic.
    """
    transform_forestfire_cost_table = SnowflakeOperator(
        task_id="transform_forestfire_cost_table",
        sql="transform_forestfire_cost_table.sql",
        params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    )

    """
    #### Quality checks
    Perform data quality checks on the various tables.
    """
    with TaskGroup(
        group_id="quality_check_group_forestfire",
        default_args={
            "conn_id": SNOWFLAKE_CONN_ID,
            "on_failure_callback": slack_failure_notification,
        },
    ) as quality_check_group_forestfire:
        """
        #### Column-level data quality check
        Run data quality checks on columns of the forestfire table
        """
        forestfire_column_checks = SQLColumnCheckOperator(
            task_id="forestfire_column_checks",
            table=SNOWFLAKE_FORESTFIRE_TABLE,
            column_mapping={
                "ID": {"null_check": {"equal_to": 0}},
                "RH": {"max": {"leq_to": 100}},
            },
        )

        """
        #### Table-level data quality check
        Run data quality checks on the forestfire table
        """
        forestfire_table_checks = SQLTableCheckOperator(
            task_id="forestfire_table_checks",
            table=SNOWFLAKE_FORESTFIRE_TABLE,
            checks={"row_count_check": {"check_statement": ROW_COUNT_CHECK}},
        )

    with TaskGroup(
        group_id="quality_check_group_cost",
        default_args={
            "conn_id": SNOWFLAKE_CONN_ID,
            "on_failure_callback": slack_failure_notification,
        },
    ) as quality_check_group_cost:
        """
        #### Column-level data quality check
        Run data quality checks on columns of the forestfire table
        """
        cost_column_checks = SQLColumnCheckOperator(
            task_id="cost_column_checks",
            table=SNOWFLAKE_COST_TABLE,
            column_mapping={
                "ID": {"null_check": {"equal_to": 0}},
                "LAND_DAMAGE_COST": {"min": {"geq_to": 0}},
                "PROPERTY_DAMAGE_COST": {"min": {"geq_to": 0}},
                "LOST_PROFITS_COST": {"min": {"geq_to": 0}},
            },
        )

        """
        #### Table-level data quality check
        Run data quality checks on the forestfire table
        """
        cost_table_checks = SQLTableCheckOperator(
            task_id="cost_table_checks",
            table=SNOWFLAKE_COST_TABLE,
            checks={"row_count_check": {"check_statement": ROW_COUNT_CHECK}},
        )

    with TaskGroup(
        group_id="quality_check_group_forestfire_costs",
        default_args={
            "conn_id": SNOWFLAKE_CONN_ID,
            "on_failure_callback": slack_failure_notification,
        },
    ) as quality_check_group_forestfire_costs:
        """
        #### Column-level data quality check
        Run data quality checks on columns of the forestfire table
        """
        forestfire_costs_column_checks = SQLColumnCheckOperator(
            task_id="forestfire_costs_column_checks",
            table=SNOWFLAKE_FORESTFIRE_COST_TABLE,
            column_mapping={"AREA": {"min": {"geq_to": 0}}},
        )

        """
        #### Table-level data quality check
        Run data quality checks on the forestfire table
        """
        forestfire_costs_table_checks = SQLTableCheckOperator(
            task_id="forestfire_costs_table_checks",
            table=SNOWFLAKE_FORESTFIRE_COST_TABLE,
            checks={
                "row_count_check": {"check_statement": ROW_COUNT_CHECK},
                "total_cost_check": {
                    "check_statement": "land_damage_cost + property_damage_cost + lost_profits_cost = total_cost"
                },
            },
        )

    """
    #### Delete tables
    Clean up the tables created for the example.
    """
    delete_forestfire_table = SnowflakeOperator(
        task_id="delete_forestfire_table",
        sql="delete_snowflake_table.sql",
        params={"table_name": SNOWFLAKE_FORESTFIRE_TABLE},
    )

    delete_cost_table = SnowflakeOperator(
        task_id="delete_costs_table",
        sql="delete_snowflake_table.sql",
        params={"table_name": SNOWFLAKE_COST_TABLE},
    )

    delete_forestfire_cost_table = SnowflakeOperator(
        task_id="delete_forestfire_cost_table",
        sql="delete_snowflake_table.sql",
        params={"table_name": SNOWFLAKE_FORESTFIRE_COST_TABLE},
    )

    begin = EmptyOperator(task_id="begin")
    create_done = EmptyOperator(task_id="create_done")
    load_done = EmptyOperator(task_id="load_done")
    end = EmptyOperator(task_id="end")

    chain(
        begin,
        [create_forestfire_table, create_cost_table, create_forestfire_cost_table],
        create_done,
        [load_forestfire_data, load_cost_data],
        load_done,
        [quality_check_group_forestfire, quality_check_group_cost],
        load_forestfire_cost_data,
        quality_check_group_forestfire_costs,
        transform_forestfire_cost_table,
        [delete_forestfire_table, delete_cost_table, delete_forestfire_cost_table],
        end,
    )
