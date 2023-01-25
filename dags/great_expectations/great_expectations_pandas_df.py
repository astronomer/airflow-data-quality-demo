"""
### Simple EL Pipeline with Data Quality Checks Using Pandas and Great Expectations

A simple example of performing data quality checks on a Pandas dataframe using Great Expectations.

What makes this a simple data quality case is:
1. Absolute ground truth: the local CSV file is considered perfect and immutable.
2. No transformations or business logic.
3. Exact values of data to quality check are known.
"""

import os
import pandas as pd

from pathlib import Path
from datetime import datetime

from airflow import DAG
from great_expectations_provider.operators.great_expectations import (
    GreatExpectationsOperator,
)

base_path = Path(__file__).parents[2]
data_file = os.path.join(
    base_path,
    "include",
    "sample_data/yellow_trip_data/yellow_tripdata_sample_2019-01.csv",
)
ge_root_dir = os.path.join(base_path, "include", "great_expectations")


with DAG(
    "great_expectations.pandas_df",
    start_date=datetime(2021, 1, 1),
    description="Example DAG showcasing loading and data quality checking with Pandas and Great Expectations.",
    doc_md=__doc__,
    schedule_interval=None,
    catchup=False,
) as dag:

    """
    #### Great Expectations suite
    Run the Great Expectations suite on the table.
    """
    ge_pandas_df_validation = GreatExpectationsOperator(
        task_id="ge_pandas_df_validation",
        data_context_root_dir=ge_root_dir,
        dataframe_to_validate=pd.read_csv(filepath_or_buffer=data_file, header=0),
        execution_engine="PandasExecutionEngine",
        expectation_suite_name="taxi.demo",
        data_asset_name="yellow_tripdata_sample_2019-01",
    )
