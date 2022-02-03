import os

from pathlib import Path
from great_expectations.core.batch import BatchRequest
from great_expectations.data_context.types.base import (
    DataContextConfig,
    CheckpointConfig
)

base_path = Path(__file__).parents[3]
data_dir = os.path.join(base_path, "include", "data")
ge_root_dir = os.path.join(base_path, "include", "great_expectations")

bigquery_data_context_config = DataContextConfig(
    **{
        "config_version": 3.0,
        "datasources": {
            "my_bigquery_datasource": {
                "data_connectors": {
                    "default_inferred_data_connector_name": {
                        "default_regex": {
                            "group_names": ["data_asset_name"],
                            "pattern": "(.*)",
                        },
                        "base_directory": data_dir,
                        "class_name": "InferredAssetFilesystemDataConnector",
                    },
                    "default_runtime_data_connector_name": {
                        "batch_identifiers": ["default_identifier_name"],
                        "class_name": "RuntimeDataConnector",
                    },
                },
                "execution_engine": {
                    "class_name": "PandasExecutionEngine",
                },
                "class_name": "Datasource",
            }
        },
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

bigquery_checkpoint_config = CheckpointConfig(
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
                    "datasource_name": "my_bigquery_datasource",
                    "data_connector_name": "default_inferred_data_connector_name",
                    "data_asset_name": "taxi",
                    "batch_spec_passthrough": {
                        "bigquery_temp_table": "taxi_temp"
                    },
                },
            }
        ],
        "profilers": [],
        "ge_cloud_id": None,
        "expectation_suite_ge_cloud_id": None,
    }
)

bigquery_batch_request = BatchRequest(
    **{
        "datasource_name": "my_bigquery_datasource",
        "data_connector_name": "default_inferred_data_connector_name",
        "data_asset_name": "great_expectations_bigquery_example.taxi",
        "data_connector_query": {"index": -1},
    }
)
