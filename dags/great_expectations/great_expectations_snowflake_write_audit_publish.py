"""
### Write-Audit-Publish Pattern EL Pipeline with Data Quality Checks Using Snowflake and Great Expectations

Use the Write-Audit-Publish pattern with Great Expectaitons and Snowflake.

Ensure a Snowflake Warehouse, Database, Schema, Role, and S3 Key and Secret
exist for the Snowflake connection, named `snowflake_default`. Access to S3
is needed for this example. An 'aws_configs' variable is needed in Variables,
see the Redshift Examples in the README section for more information.

The write-audit-publish pattern writes data to a staging table, audits the
data quality through quality checks, then publishes correct data to a
production table. In this example incorrect data is discarded, and the DAG
is failed on data quality check failure.

What makes this a simple data quality case is:
1. Absolute ground truth: the local CSV file is considered perfect and immutable.
2. No transformations or business logic.
3. Exact values of data to quality check are known.
"""

import json
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from great_expectations_provider.operators.great_expectations import \
    GreatExpectationsOperator

from include.libs.schema_reg.base_schema_transforms import \
    snowflake_load_column_string

# These variables are a placeholder. In a live environment, it is better
# to pull the info from a Variable.
table = "YELLOW_TRIPDATA"
snowflake_conn = "snowflake_default"
base_path = Path(__file__).parents[2]
table_schema_path = (
    f"{base_path}/include/sql/great_expectations_examples/table_schemas/"
)
ge_root_dir = os.path.join(base_path, "include", "great_expectations")

with DAG(
    "great_expectations.snowflake_write_audit_publish",
    start_date=datetime(2022, 1, 1),
    description="Example DAG showcasing a write-audit-publish data quality pattern with Snowflake and Great Expectations.",
    doc_md=__doc__,
    schedule_interval=None,
    template_searchpath=f"{base_path}/include/sql/snowflake_examples/",
    catchup=False,
) as dag:

    """
    #### Snowflake table creation
    Creates the tables to store sample data
    """
    create_snowflake_audit_table = SnowflakeOperator(
        task_id="create_snowflake_audit_table",
        sql="{% include 'create_snowflake_yellow_tripdata_table.sql' %}",
        params={"table_name": f"{table}_AUDIT"},
    )

    create_snowflake_table = SnowflakeOperator(
        task_id="create_snowflake_table",
        sql="{% include 'create_snowflake_yellow_tripdata_table.sql' %}",
        params={"table_name": table},
    )

    """
    #### Insert data
    Insert data into the Snowflake table using an existing SQL query (stored in
    the include/sql/snowflake_examples/ directory).
    """
    load_data = SnowflakeOperator(
        task_id="load_data",
        sql="{% include 'load_yellow_tripdata.sql' %}",
        params={"table_name": f"{table}_AUDIT"},
    )

    """
    #### Delete table
    Cleans up the tables created for the example
    """
    delete_snowflake_audit_table = SnowflakeOperator(
        task_id="delete_snowflake_audit_table",
        sql="{% include 'delete_snowflake_table.sql' %}",
        params={"table_name": f"{table}_AUDIT"},
        trigger_rule="all_success",
    )

    delete_snowflake_table = SnowflakeOperator(
        task_id="delete_snowflake_table",
        sql="{% include 'delete_snowflake_table.sql' %}",
        params={"table_name": table},
        trigger_rule="all_success",
    )

    """
    #### Great Expectations suite
    Runs the Great Expectations suite on the table
    """
    ge_snowflake_validation = GreatExpectationsOperator(
        task_id="ge_snowflake_validation",
        data_context_root_dir=ge_root_dir,
        conn_id=snowflake_conn,
        expectation_suite_name="taxi.demo",
        schema="SCHEMA", # set this to your schema
        data_asset_name=f"{table}_AUDIT",
        #fail_task_on_validation_failure=False,
    )

    with open(
        f"{table_schema_path}/tripdata_schema.json",
        "r",
    ) as f:
        table_schema = json.load(f).get("yellow_tripdata")
        table_props = table_schema.get("properties")
        table_dimensions = table_schema.get("dimensions")
        table_metrics = table_schema.get("metrics")

        col_string = snowflake_load_column_string(table_props)

        """
        #### Snowflake audit to production task
        Loads the data from the audit table to the production table
        """
        copy_snowflake_audit_to_production_table = SnowflakeOperator(
            task_id="copy_snowflake_audit_to_production_table",
            sql="{% include 'copy_yellow_tripdata_snowflake_staging.sql' %}",
            params={
                "table_name": table,
                "audit_table_name": f"{table}_AUDIT",
                "table_schema": table_props,
                "col_string": col_string,
            },
        )

    chain(
        [create_snowflake_table, create_snowflake_audit_table],
        load_data,
        ge_snowflake_validation,
        copy_snowflake_audit_to_production_table,
        [delete_snowflake_table, delete_snowflake_audit_table],
    )
