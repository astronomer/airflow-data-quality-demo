from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import datetime
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
    SQLIntervalCheckOperator,
    SQLThresholdCheckOperator
)
from airflow.utils.task_group import TaskGroup

import pandas as pd

TABLE = "yellow_tripdata"
DATES = ["2019-01", "2019-02"]

with DAG("sql_data_quality",
         start_date=datetime(2021, 7, 7),
         description="A sample Airflow DAG to perform data quality checks using SQL Operators.",
         schedule_interval=None,
         default_args={"conn_id": "postgres_default"},
         catchup=False) as dag:
    """
    ### SQL Check Operators Data Quality Example

    Before running the DAG, ensure you have an active and reachable SQL database
    running, with a connection to that database in an Airflow Connection.
    """

    begin = DummyOperator(task_id="begin")
    end = DummyOperator(task_id="end")

    """
    #### Run Row-Level Quality Checks
    For each date of data, run checks on 10 rows to ensure basic data quality
    cases (found in the .sql file) pass.
    """
    for date in DATES:
        with TaskGroup(group_id=f"row_quality_checks_{date}") as quality_check_group:
            trip_dict = pd.read_csv(
                f"/usr/local/airflow/include/data/yellow_tripdata_sample_{date}.csv",
                header=0,
                usecols=["vendor_id", "pickup_datetime"],
                parse_dates=["pickup_datetime"],
                infer_datetime_format=True
            ).to_dict()
            # Test a sample of 100 rows
            for i in range(0, (len(trip_dict["vendor_id"])-1)/1000):
                values = {}
                values["vendor_id"] = trip_dict["vendor_id"][i]
                values["pickup_datetime"] = trip_dict["pickup_datetime"][i]
                values["table"] = TABLE
                row_check = SQLCheckOperator(
                    task_id=f"forestfire_row_quality_check_{id}",
                    sql="sql/row_quality_yellow_tripdata_check.sql",
                    params=values,
                )

    """
    #### Run Table-Level Quality Check
    Ensure that the correct number of rows are present in the table.
    """
    value_check = SQLValueCheckOperator(
        task_id="check_row_count",
        sql=f"SELECT COUNT(*) FROM {TABLE}",
        pass_value=20000
    )

    """
    #### Run Interval Check
    Check that the average trip distance today is within a desirable threshold
    compared to the average trip distance yesterday.
    """
    interval_check = SQLIntervalCheckOperator(
        task_id="check_interval_data",
        days_back=1,
        date_filter_column="upload_date",
        metrics_threshold={"AVG(trip_distance)": 1.5}
    )

    """
    #### Threshold Check
    Similar to the threshold cases in the Row-Level Check above, ensures that
    certain row(s) values meet the desired threshold(s).
    """
    threshold_check = SQLThresholdCheckOperator(
        task_id="check_threshold",
        sql="SELECT MAX(passenger_count)",
        min_threshold=0,
        max_threshold=8
    )

    chain(
        begin,
        [quality_check_group, value_check, interval_check, threshold_check],
        end
    )
