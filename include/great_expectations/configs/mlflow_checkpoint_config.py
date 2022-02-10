import os

from pathlib import Path
from great_expectations.data_context.types.base import CheckpointConfig

base_path = Path(__file__).parents[3]
data_dir = os.path.join(base_path, "include", "data")
ge_root_dir = os.path.join(base_path, "include", "great_expectations")

mlflow_checkpoint_config = CheckpointConfig(
    **{
        "name": "mlflow.chk",
        "config_version": 1.0,
        "template_name": None,
        "class_name": "Checkpoint",
        "run_name_template": "%Y%m%d-%H%M%S-my-run-name-template",
        "expectation_suite_name": "mlflow.census_adult_income",
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
        "validations": [],
        "profilers": [],
        "ge_cloud_id": None,
        "expectation_suite_ge_cloud_id": None,
    }
)
