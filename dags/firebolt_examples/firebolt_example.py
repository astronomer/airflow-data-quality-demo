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
Example use of Firebolt related operators.
"""
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
#from firebolt_provider.operators.firebolt import FireboltOperator
from plugins.firebolt_operator_test import FireboltOperator

FIREBOLT_CONN_ID = 'firebolt_default'
FIREBOLT_SAMPLE_TABLE = 'order_details'
FIREBOLT_DATABASE = 'firebolt_test'
FIREBOLT_ENGINE = 'firebolt_test_general_purpose'

# SQL commands
SELECT_STATEMENT_SQL_STRING = f"SELECT * FROM {FIREBOLT_SAMPLE_TABLE} LIMIT 1;"
SQL_INSERT_STATEMENT = (
    f"INSERT INTO {FIREBOLT_SAMPLE_TABLE} values "
    f"(92,'Oil - Shortening - All - Purpose',"
    f"6928105225,5,4784.12,'2019-06-05','2019-06-05 04:02:08'); "
)
SQL_LIST = [f"SELECT * FROM {FIREBOLT_SAMPLE_TABLE} LIMIT 2;"]
SQL_CREATE_DATABASE_STATEMENT = f"CREATE DATABASE IF NOT EXISTS my_db1;"
SQL_DROP_DATABASE_STATEMENT = "DROP DATABASE IF EXISTS my_db1;"
SQL_CREATE_TABLE_STATEMENT = (
    f"CREATE FACT TABLE IF NOT EXISTS {FIREBOLT_SAMPLE_TABLE} "
    "(id INT, order_name String, order_num INT, quantity INT, price FLOAT, order_date String, order_datetime DATETIME)"
    " PRIMARY INDEX id;"
)
SQL_DROP_TABLE_STATEMENT = f"DROP TABLE IF EXISTS {FIREBOLT_SAMPLE_TABLE};"

with DAG(
    'example_firebolt',
    start_date=datetime(2021, 1, 1),
    default_args={
        'conn_id': FIREBOLT_CONN_ID,
        'hook_params': {
            'firebolt_conn_id': FIREBOLT_CONN_ID,
            'database': FIREBOLT_DATABASE,
            'engine_name': FIREBOLT_ENGINE
        }
    },
    tags=['example'],
    catchup=False,
) as dag:

    firebolt_op_sql_str = FireboltOperator(
        task_id='firebolt_op_sql_str',
        sql=SELECT_STATEMENT_SQL_STRING,
    )

    firebolt_op_with_params = FireboltOperator(
        task_id='firebolt_op_with_params',
        sql=SQL_INSERT_STATEMENT,
    )

    firebolt_op_sql_list = FireboltOperator(
        task_id='firebolt_op_sql_list',
        sql=SQL_LIST,
    )

    firebolt_op_sql_create_db = FireboltOperator(
        task_id='firebolt_op_sql_create_db',
        sql=SQL_CREATE_DATABASE_STATEMENT,
    )

    firebolt_op_sql_drop_db = FireboltOperator(
        task_id='firebolt_op_sql_drop_db',
        sql=SQL_DROP_DATABASE_STATEMENT,
    )

    firebolt_op_sql_create_table = FireboltOperator(
        task_id='firebolt_op_sql_create_table',
        sql=SQL_CREATE_TABLE_STATEMENT,
    )

    firebolt_op_sql_drop_table = FireboltOperator(
        task_id='firebolt_op_sql_drop_table',
        sql=SQL_DROP_TABLE_STATEMENT,
    )

    chain(
        firebolt_op_sql_create_db,
        firebolt_op_sql_create_table,
        firebolt_op_with_params,
        firebolt_op_sql_str,
        firebolt_op_sql_list,
        firebolt_op_sql_drop_table,
        firebolt_op_sql_drop_db,
    )
