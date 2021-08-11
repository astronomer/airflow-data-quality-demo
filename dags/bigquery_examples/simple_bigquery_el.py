from airflow import DAG, AirflowException
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
    BigQueryCheckOperator,
    BigQueryValueCheckOperator,
    BigQueryDeleteDatasetOperator
)
from airflow.utils.dates import datetime
from airflow.utils.task_group import TaskGroup

import json


DATASET='simple_bigquery_example_dag'
TABLE='forestfires'
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': datetime(2021, 1, 1),
    'email': ['noreply@astronomer.io'],
    'email_on_failure': False
}

with DAG('simple_bigquery_el',
         default_args=default_args,
         description='Example DAG showcasing loading and data quality checking with BigQuery.',
         schedule_interval=None,
         catchup=False) as dag:
    """
    ### Simple EL Pipeline with Data Integrity and Quality Checks

    Before running the DAG, set the following in an Airflow or Environment Variable:
    - key: gcp_project_id
      value: [gcp_project_id]
    - key: connector_id
      value: [connector_id]
    - key: location
      value: [project_location]
    Fully replacing [gcp_project_id] & [connector_id] with the actual IDs.

    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2. No transformations or business logic.
    3. Exact values of data to quality check are known.

    Tasks:
        1. Create dataset with BigQueryCreateEmptyDatasetOperator
        2. Create table with BigQueryCreateEmptyTableOperator
        3. Check table exists with BigQueryTableExistenceSensor
        3. Insert data to table with BigQueryInsertJobOperator
        4. Validate data with
            a. BigQueryValueCheckOperator for row count
            b. BigQueryCheckOperator for data validation
        5. Delete dataset with BigQueryDeleteDatasetOperator
    """

    """
    #### BigQuery dataset creation
    Create the dataset to store the sample data tables.
    """
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME
    )

    """
    #### BigQuery table creation
    Create the table to store sample forest fire data.
    """
    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET,
        table_id=TABLE,
        schema_fields=[
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "y", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "month", "type": "STRING", "mode": "NULLABLE"},
            {"name": "day", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ffmc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dmc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "dc", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "isi", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "temp", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rh", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "wind", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "rain", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "area", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    """
    #### BigQuery table check
    Ensure that the table was created in BigQuery before inserting data.
    """
    check_table_exists = BigQueryTableExistenceSensor(
        task_id='check_for_table',
        project_id=Variable.get('gcp_project_id'),
        dataset_id=DATASET,
        table_id=TABLE,
    )

    """
    #### Insert data
    Insert data into the BigQuery table using an existing SQL query (stored in
    a file under dags/sql).
    """
    load_data = BigQueryInsertJobOperator(
        task_id="insert_query",
        configuration={
            "query": {
                "query": "{% include 'load_bigquery_forestfire_data.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    """
    #### Row-level data quality check
    Run data quality checks on a few rows, ensuring that the data in BigQuery
    matches the ground truth in the correspoding JSON file.
    """
    with open("include/validation/forestfire_validation.json") as ffv:
        with TaskGroup(group_id="row_quality_checks") as quality_check_group:
            ffv_json = json.load(ffv)
            for id, values in ffv_json.items():
                BigQueryCheckOperator(
                    task_id="check_row_data",
                    sql=f"""
                        SELECT ID,
                          CASE WHEN y {{ 'IS ' + values['y'] if values['y'] == 'NULL' else '= ' + values['y'] }} THEN 1 ELSE 0 END AS y_check,
                          CASE WHEN month  {{ 'IS ' + values['month'] if values['month'] == 'NULL' else '= ' + values['month'] }} THEN 1 ELSE 0 END AS month_check,
                          CASE WHEN day {{ 'IS ' + values['day'] if values['day'] == 'NULL' else '= ' + values['day'] }} THEN 1 ELSE 0 END AS day_check,
                          CASE WHEN ffmc {{ 'IS ' + values['ffmc'] if values['ffmc'] == 'NULL' else '= ' + values['ffmc'] }} THEN 1 ELSE 0 END AS ffmc_check,
                          CASE WHEN dmc {{ 'IS ' + values['dmc'] if values['dmc'] == 'NULL' else '= ' + values['dmc'] }} THEN 1 ELSE 0 END AS dmc_check,
                          CASE WHEN dc {{ 'IS ' + values['dc'] if values['dc'] == 'NULL' else '= ' + values['dc'] }} THEN 1 ELSE 0 END AS dc_check,
                          CASE WHEN isi {{ 'IS ' + values['isi'] if values['isi'] == 'NULL' else '= ' + values['isi'] }} THEN 1 ELSE 0 END AS isi_check,
                          CASE WHEN temp {{ 'IS ' + values['temp'] if values['temp'] == 'NULL' else '= ' + values['temp'] }} THEN 1 ELSE 0 END AS temp_check,
                          CASE WHEN rh {{ 'IS ' + values['rh'] if values['rh'] == 'NULL' else '= ' + values['rh'] }} THEN 1 ELSE 0 END AS rh_check,
                          CASE WHEN wind {{ 'IS ' + values['wind'] if values['wind'] == 'NULL' else '= ' + values['wind'] }} THEN 1 ELSE 0 END AS wind_check,
                          CASE WHEN rain {{ 'IS ' + values['rain'] if values['rain'] == 'NULL' else '= ' + values['rain'] }} THEN 1 ELSE 0 END AS rain_check,
                          CASE WHEN area {{ 'IS ' + values['area'] if values['area'] == 'NULL' else '= ' + values['area'] }} THEN 1 ELSE 0 END AS area_check
                        FROM {DATASET.TABLE}
                        WHERE ID = {id}
                    """,
                    use_legacy_sql=False
                )

    """
    #### Table-level data quality check
    Run a row count check to ensure all data was uploaded to BigQuery properly.
    """
    check_bq_row_count = BigQueryValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM {DATASET}.{TABLE}",
        pass_value=9,
        use_legacy_sql=False,
    )

    """
    #### Delete test dataset and table
    Clean up the dataset and table created for the example.
    """
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET,
        delete_contents=True
    )

    begin = DummyOperator(task_id='begin')
    end = DummyOperator(task_id='end')

    begin >> create_dataset >> create_table >> check_table_exists >> load_data
    load_data >> check_row_data >> delete_dataset
    load_data >> check_bq_row_count >> delete_dataset
    delete_dataset >> end
