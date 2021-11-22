import os
import ast

from airflow.utils.dates import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDatasetTablesOperator, BigQueryInsertJobOperator)
from airflow.providers.google.cloud.transfers.bigquery_to_bigquery import BigQueryToBigQueryOperator


DBT_PROJ_DIR = os.getenv('DBT_PROJECT_DIR')
DBT_PROFILE_DIR = os.getenv('DBT_PROFILE_DIR')
DATASET = 'simple_bigquery_example_dag'
AUDIT_PATH = f'{DATASET}_dbt_test__audit'
FAIL_TABLE = 'accepted_values_forestfire_test_month__aug__mar__sep'

with DAG('dbt_test_store_failures',
         start_date=datetime(2021, 10, 8),
         template_searchpath='/usr/local/airflow/include/sql/dbt_examples/',
         schedule_interval=None) as dag:
    """
    DAG to run dbt project and tests, then load the store_failures table into
    a permament table so subsequent runs do not overwrite.

    For the DAG to work, the following must exist:
      - An Airflow Connection to GCP and BigQuery
      - A BigQuery Dataset and Table created with forestfire data (can be
          created by running the bigquery_examples.simple_bigquery_el DAG)
      - A dbt profile with a connection to BigQuery in include/dbt/.dbt (.dbt
          directory is .gitignored, this must be generated)
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

    """
    Get the store_failures tables from dbt test failures
    """
    get_dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id='get_dataset_tables',
        dataset_id=AUDIT_PATH,
        trigger_rule='one_failed'
    )

    """
    Copy data from each store_failures table

    Until (AIP-42)[https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-42%3A+Dynamic+Task+Mapping]
    is implemented, each task must be hard-coded. One is given as an example.
    """
    copy_store_failures = BigQueryToBigQueryOperator(
        task_id=f'copy_store_failures',
        source_project_dataset_tables=f'{AUDIT_PATH}.{FAIL_TABLE}',
        destination_project_dataset_table=f'{AUDIT_PATH}_permanent.{FAIL_TABLE}',
        write_disposition='WRITE_APPEND',
        create_disposition='CREATE_IF_NEEDED'
    )

    dbt_run >> dbt_test >> get_dataset_tables >> copy_store_failures
