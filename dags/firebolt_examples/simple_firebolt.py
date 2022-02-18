#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Example use of Firebolt operator.
"""
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.sql import SQLCheckOperator
from airflow.utils.task_group import TaskGroup
from firebolt_provider.operators.firebolt import FireboltOperator

FIREBOLT_CONN_ID = "firebolt_default"
FIREBOLT_SAMPLE_TABLE = "yellow_tripdata"
FIREBOLT_DATABASE = "firebolt_test"
FIREBOLT_ENGINE = "firebolt_test_general_purpose"

# TODO: Finish filling in the checks
checks = {
    "check_name": "statement"
}

with DAG(
    "simple_firebolt",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args={
        "conn_id": FIREBOLT_CONN_ID,
        "firebolt_conn_id": FIREBOLT_CONN_ID,
        "database": FIREBOLT_DATABASE,
        "engine_name": FIREBOLT_ENGINE
    },
    template_searchpath="/usr/local/airflow/include/sql/firebolt_examples/",
    catchup=False,
) as dag:

    create_table = FireboltOperator(
        task_id="create_table",
        sql="create_table.sql",
        params={"table": FIREBOLT_SAMPLE_TABLE}
    )

    # TODO: write this sql file or change the operator to better load data
    load_data = FireboltOperator(
        task_id="load_data",
        sql="load_data.sql"
    )

    with TaskGroup(group_id="aggregate_quality_checks") as check_group:
        for name, statement in checks:
            check = SQLCheckOperator(
                task_id=f"check_{name}",
                sql="row_quality_yellow_tripdata_template.sql",
                params={"col": name, "check_statement": statement},
                hook_params={}
            )

    drop_table = FireboltOperator(
        task_id="drop_table",
        sql="drop_table.sql",
        params={"table": FIREBOLT_SAMPLE_TABLE}
    )

    chain(
        create_table,
        load_data,
        check_group,
        drop_table,
    )
