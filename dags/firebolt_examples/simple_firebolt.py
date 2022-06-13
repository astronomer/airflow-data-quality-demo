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
Example use of Firebolt operators.
"""
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.sql import SQLCheckOperator
from airflow.utils.task_group import TaskGroup
from firebolt_provider.operators.firebolt import (
    FireboltOperator, FireboltStartEngineOperator, FireboltStopEngineOperator
)

FIREBOLT_CONN_ID = "firebolt_default"
FIREBOLT_SAMPLE_TABLE = "forest_fire"
FIREBOLT_DATABASE = "firebolt_test"
FIREBOLT_ENGINE = "firebolt_test_general_purpose"

CHECKS = {
    "id": "'column' IS NOT NULL",
    "ffmc": "MAX(ffmc) < 100"
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
    """Example Firebolt Data Quality DAG

    DAG starts the Firebolt engine specificed, creates sample table, loads sample data into table,
    runs quality checks in CHECKS dictionary, then deletes the table and stops the engine.

    Checks work by running a MIN() function over the specific aggregate check, where the aggregate
    check is contained in a CASE statement. The CASE statement checks the result of the condition;
    if true, the CASE statement returns 1, else 0. Then MIN() will return 0 if any row returns a
    false result, and true otherwise.

    Note: the Firebolt operator currently does not support templated SQL queries.
    """

    start_engine = FireboltStartEngineOperator(
        task_id="start_engine"
    )

    create_table = FireboltOperator(
        task_id="create_table",
        sql=f"""
        CREATE DIMENSION TABLE {FIREBOLT_SAMPLE_TABLE}
        (id INT,
            y INT,
            month VARCHAR(25),
            day VARCHAR(25),
            ffmc FLOAT,
            dmc FLOAT,
            dc FLOAT,
            isi FLOAT,
            temp FLOAT,
            rh FLOAT,
            wind FLOAT,
            rain FLOAT,
            area FLOAT);""",
    )

    load_data = FireboltOperator(
        task_id="load_data",
        sql=f"""
        INSERT INTO {FIREBOLT_SAMPLE_TABLE} VALUES
        (1,2,'aug','fri',91,166.9,752.6,7.1,25.9,41,3.6,0,0),
        (2,2,'feb','mon',84,9.3,34,2.1,13.9,40,5.4,0,0),
        (3,4,'mar','sat',69,2.4,15.5,0.7,17.4,24,5.4,0,0),
        (4,4,'mar','mon',87.2,23.9,64.7,4.1,11.8,35,1.8,0,0),
        (5,5,'mar','sat',91.7,35.8,80.8,7.8,15.1,27,5.4,0,0),
        (6,5,'sep','wed',92.9,133.3,699.6,9.2,26.4,21,4.5,0,0),
        (7,5,'mar','fri',86.2,26.2,94.3,5.1,8.2,51,6.7,0,0),
        (8,6,'mar','fri',91.7,33.3,77.5,9,8.3,97,4,0.2,0),
        (9,9,'feb','thu',84.2,6.8,26.6,7.7,6.7,79,3.1,0,0);
        """
    )

    with TaskGroup(group_id="aggregate_quality_checks") as check_group:
        for name, statement in CHECKS.items():
            check = SQLCheckOperator(
                task_id=f"check_{name}",
                sql="quality_check_template.sql",
                params={"col": name, "check_statement": statement, "table": FIREBOLT_SAMPLE_TABLE},
            )

    drop_table = FireboltOperator(
        task_id="drop_table",
        sql="DROP TABLE IF EXISTS forest_fire;",
    )

    stop_engine = FireboltStopEngineOperator(
        task_id="stop_engine"
    )

    chain(
        start_engine,
        create_table,
        load_data,
        check_group,
        drop_table,
        stop_engine,
    )
