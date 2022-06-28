from typing import Any, Dict, Optional, Union, List

from airflow.providers.snowflake.operators.snowflake import get_db_hook as get_snowflake_hook
from include.common.sql.operators.sql import SQLColumnCheckOperator, SQLTableCheckOperator

class SnowflakeColumnCheckOperator(SQLColumnCheckOperator):
    def __init__(
        self,
        *,
        table: str,
        column_mapping: Dict[str, List[str]],
        conn_id: str = 'snowflake_default',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        role: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(
            table=table,
            column_mapping=column_mapping,
            **kwargs
        )
        self.snowflake_conn_id = conn_id
        self.table = table
        self.column_mapping = column_mapping
        self.autocommit = autocommit
        self.do_xcom_push = do_xcom_push
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.query_ids: List[str] = []

    def get_db_hook(self):
        return get_snowflake_hook(self)

class SnowflakeTableCheckOperator(SQLTableCheckOperator):
    def __init__(
        self,
        *,
        table: str,
        checks: Dict[str, Dict[str, Any]],
        conn_id: str = 'snowflake_default',
        parameters: Optional[dict] = None,
        autocommit: bool = True,
        do_xcom_push: bool = True,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        role: Optional[str] = None,
        schema: Optional[str] = None,
        authenticator: Optional[str] = None,
        session_parameters: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(
            table=table,
            checks=checks,
            **kwargs
        )
        self.snowflake_conn_id = conn_id
        self.table = table
        self.checks = checks
        self.autocommit = autocommit
        self.do_xcom_push = do_xcom_push
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.query_ids = []

    def get_db_hook(self):
        return get_snowflake_hook(self)