import os
import yaml

from pathlib import Path
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig
)

base_path = Path(__file__).parents[3]
data_dir = os.path.join(base_path, "include", "data")
ge_root_dir = os.path.join(base_path, "include", "great_expectations")
connection_string = ""

# Note: The user must first configure a `config_variable.yml` file for this to work
# The file is not included with this repo.
with open(
    f"{ge_root_dir}/uncommitted/config_variables.yml",
    "r",
) as f:
    connection_string = yaml.safe_load(f).get("my_snowflake_db")

snowflake_data_context_config = DataContextConfig(
    **{
        "config_version": 3.0,
        "datasources": {},
        "config_variables_file_path": os.path.join(
            ge_root_dir, "uncommitted", "config_variables.yml"
        ),
        "stores": {
            "expectations_store": {
                "class_name": "ExpectationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(ge_root_dir, "expectations"),
                },
            },
            "validations_store": {
                "class_name": "ValidationsStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(
                        ge_root_dir, "uncommitted", "validations"
                    ),
                },
            },
            "evaluation_parameter_store": {"class_name": "EvaluationParameterStore"},
            "checkpoint_store": {
                "class_name": "CheckpointStore",
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "suppress_store_backend_id": True,
                    "base_directory": os.path.join(ge_root_dir, "checkpoints"),
                },
            },
        },
        "expectations_store_name": "expectations_store",
        "validations_store_name": "validations_store",
        "evaluation_parameter_store_name": "evaluation_parameter_store",
        "checkpoint_store_name": "checkpoint_store",
        "data_docs_sites": {
            "local_site": {
                "class_name": "SiteBuilder",
                "show_how_to_buttons": True,
                "store_backend": {
                    "class_name": "TupleFilesystemStoreBackend",
                    "base_directory": os.path.join(
                        ge_root_dir, "uncommitted", "data_docs", "local_site"
                    ),
                },
                "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
            }
        },
        "anonymous_usage_statistics": {
            "data_context_id": "abcdabcd-1111-2222-3333-abcdabcdabcd",
            "enabled": False,
        },
        "notebooks": None,
        "concurrency": {"enabled": False},
    }
)

snowflake_checkpoint_config = CheckpointConfig(
    **{
        "name": "taxi.pass.chk",
        "config_version": 1.0,
        "template_name": None,
        "module_name": "great_expectations.checkpoint",
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "expectation_suite_name": "taxi.demo",
        "batch_request": None,
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
            },
        ],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_snowflake_datasource",
                    "data_connector_name": "default_inferred_data_connector_name",
                    "data_asset_name": "yellow_tripdata",
                },
            }
        ],
        "profilers": [],
        "ge_cloud_id": None,
        "expectation_suite_ge_cloud_id": None,
    }
)

snowflake_audit_checkpoint_config = CheckpointConfig(
    **{
        "name": "taxi.pass.chk",
        "config_version": 1.0,
        "template_name": None,
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "expectation_suite_name": "taxi.demo",
        "batch_request": None,
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "store_evaluation_params",
                "action": {"class_name": "StoreEvaluationParametersAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []},
            },
        ],
        "evaluation_parameters": {},
        "runtime_configuration": {},
        "validations": [
            {
                "batch_request": {
                    "datasource_name": "my_snowflake_datasource",
                    "data_connector_name": "default_inferred_data_connector_name",
                    "data_asset_name": "yellow_tripdata_audit",
                },
            }
        ],
        "profilers": [],
        "ge_cloud_id": None,
        "expectation_suite_ge_cloud_id": None,
    }
)

snowflake_batch_request = BatchRequest(
    **{
        "datasource_name": "my_snowflake_db",
        "data_connector_name": "default_inferred_data_connector_name",
        "data_asset_name": "yellow_tripdata_sample_2019-01.csv",
        "data_connector_query": {"index": -1},
    }
)
