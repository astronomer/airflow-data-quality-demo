import os

from airflow.utils.dates import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.utils.task_group import TaskGroup


DBT_PROJ_DIR = os.getenv('DBT_PROJECT_DIR_SNOWFLAKE')
DBT_PROFILE_DIR = os.getenv('DBT_PROFILE_DIR')
SCHEMA = 'SANDBOX_BENJI'
AUDIT_PATH = f'{SCHEMA}_DBT_TEST__AUDIT'
MONTH_FAIL_TABLE = 'ACCEPTED_VALUES_FORESTFIRE_TEST_MONTH__AUG__MAR__SEP'
FFMC_FAIL_TABLE = 'FFMC_VALUE_CHECK_FORESTFIRE_TEST_FFMC'


with DAG('dbt.copy_store_failures_snowflake',
         start_date=datetime(2021, 10, 8),
         template_searchpath='/usr/local/airflow/include/sql/dbt_examples/',
         schedule_interval=None) as dag:
    """
    DAG to run dbt project and tests, then load the store_failures table into
    a permament table so subsequent runs do not overwrite.

    For the DAG to work, the following must exist:
      - An Airflow Connection to Snowflake
      - A Snowflake Schema and Table created with forestfire data (can be
          created by running the snowflake_examples.simple__el DAG)
      - A dbt profile with a connection to Snowflake in include/dbt/.dbt (.dbt
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
    Copy data from each store_failures table

    Until (AIP-42)[https://cwiki.apache.org/confluence/display/AIRFLOW/AIP-42%3A+Dynamic+Task+Mapping]
    is implemented, each task must be hard-coded. One is given as an example.
    """
    with TaskGroup(group_id='copy_store_failures_group') as copy_store_failures_group:
        copy_test_month = SnowflakeOperator(
            task_id='copy_test_month',
            sql='{% include "copy_store_failures_snowflake.sql" %}',
            params={
                'source_table': f'"{AUDIT_PATH}"."{MONTH_FAIL_TABLE}"',
                'destination_table': f'"{SCHEMA}"."{MONTH_FAIL_TABLE}"',
                'columns': 'VALUE_FIELD, N_RECORDS'
            },
            trigger_rule='one_failed'
        )

        copy_test_ffmc = SnowflakeOperator(
            task_id='copy_test_ffmc',
            sql='{% include "copy_store_failures_snowflake.sql" %}',
            params={
                'source_table': f'"{AUDIT_PATH}"."{FFMC_FAIL_TABLE}"',
                'destination_table': f'"{SCHEMA}"."{FFMC_FAIL_TABLE}"',
                'columns': 'FFMC'
            },
            trigger_rule='one_failed'
        )

    dbt_run >> dbt_test >> copy_store_failures_group
