## Source https://medium.com/apache-airflow/airflow-2-0-dag-authoring-redesigned-651edc397178

from typing import Any
from airflow.models.xcom import BaseXCom
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import pandas as pd
import uuid


class GCSXComBackend(BaseXCom):
    PREFIX = "xcom_gcs://"
    BUCKET_NAME = "xcom_gcs"

    @staticmethod
    def serialize_value(value: Any):
        if isinstance(value, pd.DataFrame):
            hook = GCSHook()
            object_name = "data_" + str(uuid.uuid4())
            with hook.provide_file_and_upload(
                    bucket_name=GCSXComBackend.BUCKET_NAME,
                    object_name=object_name,
            ) as f:
                value.to_csv(f.name, index=False)
            # Append prefix to persist information that the file
            # has to be downloaded from GCS
            value = GCSXComBackend.PREFIX + object_name
        return BaseXCom.serialize_value(value)

    @staticmethod
    def deserialize_value(result) -> Any:
        result = BaseXCom.deserialize_value(result)
        if isinstance(result, str) and result.startswith(GCSXComBackend.PREFIX):
            object_name = result.replace(GCSXComBackend.PREFIX, "")
            with GCSHook().provide_file(
                    bucket_name=GCSXComBackend.BUCKET_NAME,
                    object_name=object_name,
            ) as f:
                f.flush()
                result = pd.read_csv(f.name)
        return result
