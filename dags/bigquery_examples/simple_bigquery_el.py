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
    ### Simple EL Pipeline with Data Quality Checks Using BigQuery

    Before running the DAG, set the following in an Airflow or Environment Variable:
    - key: gcp_project_id
      value: [gcp_project_id]
    Fully replacing [gcp_project_id] with the actual ID.

    Ensure you have a connection to GCP, using a role with access to BigQuery
    and the ability to create, modify, and delete datasets and tables.

    What makes this a simple data quality case is:
    1. Absolute ground truth: the local CSV file is considered perfect and immutable.
    2. No transformations or business logic.
    3. Exact values of data to quality check are known.
    """

    """
    #### BigQuery dataset creation
    Create the dataset to store the sample data tables.
    """
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET
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
                "query": f"""
                    INSERT {DATASET}.{TABLE} VALUES
                      (1,2,'aug','fri',91,166.9,752.6,7.1,25.9,41,3.6,0,0),
                      (2,2,'feb','mon',84,9.3,34,2.1,13.9,40,5.4,0,0),
                      (3,4,'mar','sat',69,2.4,15.5,0.7,17.4,24,5.4,0,0),
                      (4,4,'mar','mon',87.2,23.9,64.7,4.1,11.8,35,1.8,0,0),
                      (5,5,'mar','sat',91.7,35.8,80.8,7.8,15.1,27,5.4,0,0),
                      (6,5,'sep','wed',92.9,133.3,699.6,9.2,26.4,21,4.5,0,0),
                      (7,5,'mar','fri',86.2,26.2,94.3,5.1,8.2,51,6.7,0,0),
                      (8,6,'mar','fri',91.7,33.3,77.5,9,8.3,97,4,0.2,0),
                      (9,9,'feb','thu',84.2,6.8,26.6,7.7,6.7,79,3.1,0,0);
                """,
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
                    task_id=f"check_row_data_{id}",
                    sql=f"""
                        SELECT ID,
                          CASE y WHEN {values['y']} THEN 1 ELSE 0 END AS y_check,
                          CASE month WHEN '{values['month']}' THEN 1 ELSE 0 END AS month_check,
                          CASE day WHEN '{values['day']}' THEN 1 ELSE 0 END AS day_check,
                          CASE ffmc WHEN {values['ffmc']} THEN 1 ELSE 0 END AS ffmc_check,
                          CASE dmc WHEN {values['dmc']} THEN 1 ELSE 0 END AS dmc_check,
                          CASE dc WHEN {values['dc']} THEN 1 ELSE 0 END AS dc_check,
                          CASE isi WHEN {values['isi']} THEN 1 ELSE 0 END AS isi_check,
                          CASE temp WHEN {values['temp']} THEN 1 ELSE 0 END AS temp_check,
                          CASE rh WHEN {values['rh']} THEN 1 ELSE 0 END AS rh_check,
                          CASE wind WHEN {values['wind']} THEN 1 ELSE 0 END AS wind_check,
                          CASE rain WHEN {values['rain']} THEN 1 ELSE 0 END AS rain_check,
                          CASE area WHEN {values['area']} THEN 1 ELSE 0 END AS area_check
                        FROM {DATASET}.{TABLE}
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
    load_data >> quality_check_group >> delete_dataset
    load_data >> check_bq_row_count >> delete_dataset
    delete_dataset >> end
