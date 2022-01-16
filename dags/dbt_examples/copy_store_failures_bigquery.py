import os
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import (
    BigQueryToBigQueryOperator)
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDatasetTablesOperator)
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


DBT_PROJ_DIR = os.getenv('DBT_PROJECT_DIR_BIGQUERY')
DBT_PROFILE_DIR = os.getenv('DBT_PROFILE_DIR')
DATASET = 'simple_bigquery_example_dag'
AUDIT_PATH = f'{DATASET}_dbt_test__audit'


with DAG('dbt.copy_store_failures_bigquery',
         start_date=datetime(2021, 10, 8),
         schedule_interval=None) as dag:
    """
    DAG to run dbt project and tests, then load the store_failures table into
    a permament table so subsequent runs do not overwrite.

    For the DAG to work, the following must exist:
      - An Airflow Connection to GCP and BigQuery
      - A BigQuery Dataset and Table created with forestfire data (can be
          created by running the bigquery_examples.simple_bigquery_el DAG)
      - A dbt profile with a connection to BigQuery in include/dbt/.dbt (.dbt
          directory is in .gitignore, this must be generated)
    """

    """
    Run the dbt suite
    """
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"""
        dbt run \
        --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJ_DIR}
        """
    )

    """
    Run dbt test suite
    """
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f"""
        dbt test --vars 'date: {{{{yesterday_ds}}}}' \
        --profiles-dir {DBT_PROFILE_DIR} --project-dir {DBT_PROJ_DIR}
        """
    )

    get_store_failures_tables = BigQueryGetDatasetTablesOperator(
        dataset_id=DATASET
    )

    """
    Copy data from each store_failures table
    """
    with TaskGroup(
        group_id='copy_store_failures_group',
        default_args={
            'trigger_rule': TriggerRule.ONE_FAILED,
            'write_disposition': 'WRITE_APPEND',
            'create_disposition': 'CREATE_IF_NEEDED',
        }
    ) as copy_store_failures_group:
        """
        This line is where we need AIP-42 to pull the result of get_store_failures_tables
        and create a BigQueryToBigQueryOperator task for each table returned.
        """
        for store_failures_table in get_store_failures_tables.xcom_pull():
            BigQueryToBigQueryOperator(
                task_id=f'copy_{store_failures_table}',
                source_project_dataset_tables=f'{AUDIT_PATH}.{store_failures_table}',
                destination_project_dataset_table=f'{AUDIT_PATH}_permanent.{store_failures_table}'
            )

    dbt_run >> dbt_test >> get_store_failures_tables >> copy_store_failures_group
