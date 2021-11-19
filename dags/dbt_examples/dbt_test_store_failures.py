import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import datetime


DBT_PROJ_DIR = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILE_DIR = os.getenv("DBT_PROFILE_DIR")

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
    Insert data to store_failures table
    """
    """
    insert_store_failures = BigQueryInsertJobOperator(
        task_id="insert_query",
        configuration={
            "query": {
                "query": "{% include 'load_bigquery_store_failures.sql' %}",
                "useLegacySql": False,
            }
        },
    )
    """

    dbt_run >> dbt_test  # >> insert_store_failures
