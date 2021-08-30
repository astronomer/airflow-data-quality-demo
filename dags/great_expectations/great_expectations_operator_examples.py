"""
A DAG that demonstrates use of the operators in this provider package.
"""

import os

from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.models.baseoperator import chain
from great_expectations_provider.operators.great_expectations import GreatExpectationsOperator


base_path = "/usr/local/airflow/"
data_file = os.path.join(
    base_path, "include", "data/yellow_tripdata_sample_2019-01.csv"
)
ge_root_dir = os.getenv("GE_DATA_CONTEXT_ROOT_DIR")

with DAG(
    dag_id="example_great_expectations_dag",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args={"data_context_root_dir": ge_root_dir}
) as dag:

    # This runs an expectation suite against a sample data asset. You may need to change these paths if you do not have your `data`
    # directory living in a top-level `include` directory. Ensure the checkpoint yml files have the correct path to the data file.

    # This runs an expectation suite against a data asset that passes the tests
    ge_batch_kwargs_list_pass = GreatExpectationsOperator(
        task_id="ge_batch_kwargs_list_pass",
        assets_to_validate=[
            {
                "batch_kwargs": {"path": data_file, "datasource": "data__dir"},
                "expectation_suite_name": "taxi.demo",
            }
        ]
    )

    # This runs a checkpoint and passes in a root dir.
    ge_checkpoint_pass_root_dir = GreatExpectationsOperator(
        task_id="ge_checkpoint_pass_root_dir",
        run_name="ge_airflow_run",
        checkpoint_name="taxi.pass.chk"
    )

    ge_batch_kwargs_pass = GreatExpectationsOperator(
        task_id="ge_batch_kwargs_pass",
        expectation_suite_name="taxi.demo",
        batch_kwargs={"path": data_file, "datasource": "data__dir"}
    )

    # This runs a checkpoint that will fail, but we set a flag to exit the task successfully.
    ge_checkpoint_fail_but_continue = GreatExpectationsOperator(
        task_id="ge_checkpoint_fail_but_continue",
        run_name="ge_airflow_run",
        checkpoint_name="taxi.fail.chk",
        fail_task_on_validation_failure=False
    )

    # This runs a checkpoint that will pass. Make sure the checkpoint yml file has the correct path to the data file.
    ge_checkpoint_pass = GreatExpectationsOperator(
        task_id="ge_checkpoint_pass",
        run_name="ge_airflow_run",
        checkpoint_name="taxi.pass.chk"
    )

    # This runs a checkpoint that will fail. Make sure the checkpoint yml file has the correct path to the data file.
    ge_checkpoint_fail = GreatExpectationsOperator(
        task_id="ge_checkpoint_fail",
        run_name="ge_airflow_run",
        checkpoint_name="taxi.fail.chk"
    )

    chain(
        ge_batch_kwargs_list_pass, ge_checkpoint_pass_root_dir, ge_batch_kwargs_pass,
        ge_checkpoint_fail_but_continue, ge_checkpoint_pass, ge_checkpoint_fail
    )
