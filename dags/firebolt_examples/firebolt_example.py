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
from firebolt_provider.operators.firebolt import FireboltOperator

FIREBOLT_CONN_ID = "firebolt_default"
FIREBOLT_SAMPLE_TABLE = "order_details"
FIREBOLT_DATABASE = "firebolt_test"
FIREBOLT_SAMPLE_DATABASE = "firebolt_test_1"
FIREBOLT_ENGINE = "firebolt_test_general_purpose"

with DAG(
    'firebolt_example',
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    default_args={
        'conn_id': FIREBOLT_CONN_ID,
        'firebolt_conn_id': FIREBOLT_CONN_ID,
        'database': FIREBOLT_DATABASE,
        'engine_name': FIREBOLT_ENGINE
    },
    template_searchpath="/usr/local/airflow/include/sql/firebolt_examples/",
    catchup=False,
) as dag:

    firebolt_op_create_db = FireboltOperator(
        task_id='firebolt_op_create_db',
        sql="create_database.sql",
        params={"db": FIREBOLT_SAMPLE_DATABASE}
    )

    firebolt_op_create_table = FireboltOperator(
        task_id='firebolt_op_create_table',
        sql="create_table.sql",
        params={"table": FIREBOLT_SAMPLE_TABLE}
    )

    firebolt_op_insert_with_params = FireboltOperator(
        task_id='firebolt_op_insert_with_params',
        sql="insert.sql",
        params={
            "table": FIREBOLT_SAMPLE_TABLE,
            "order_num": 6928105225
        }
    )

    firebolt_op_select = FireboltOperator(
        task_id='firebolt_op_select',
        sql="select.sql",
        params={"table": FIREBOLT_SAMPLE_TABLE}
    )

    firebolt_op_list = FireboltOperator(
        task_id='firebolt_op_list',
        sql="list.sql",
        params={"table": FIREBOLT_SAMPLE_TABLE}
    )

    firebolt_op_drop_table = FireboltOperator(
        task_id='firebolt_op_drop_table',
        sql="drop_table.sql",
        params={"table": FIREBOLT_SAMPLE_TABLE}
    )

    firebolt_op_drop_db = FireboltOperator(
        task_id='firebolt_op_drop_db',
        sql="drop_database.sql",
        params={"db": FIREBOLT_SAMPLE_DATABASE}
    )

    chain(
        firebolt_op_create_db,
        firebolt_op_create_table,
        firebolt_op_insert_with_params,
        firebolt_op_select,
        firebolt_op_list,
        firebolt_op_drop_table,
        firebolt_op_drop_db,
    )
