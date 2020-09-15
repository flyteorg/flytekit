import json as _json
import os as _os

from flytekit.common.constants import DistributedTrainingContextKey as _DistributedTrainingContextKey

SM_RESOURCE_CONFIG_FILE = "/opt/ml/input/config/resourceconfig.json"
SM_ENV_VAR_CURRENT_HOST = "SM_CURRENT_HOST"
SM_ENV_VAR_HOSTS = "SM_HOSTS"
SM_ENV_VAR_NETWORK_INTERFACE_NAME = "SM_NETWORK_INTERFACE_NAME"


def get_sagemaker_distributed_training_context_from_env() -> dict:
    distributed_training_context = {}
    if (
        not _os.environ.get(SM_ENV_VAR_CURRENT_HOST)
        or not _os.environ.get(SM_ENV_VAR_HOSTS)
        or _os.environ.get(SM_ENV_VAR_NETWORK_INTERFACE_NAME)
    ):
        raise KeyError

    distributed_training_context[_DistributedTrainingContextKey.CURRENT_HOST] = _os.environ.get(SM_ENV_VAR_CURRENT_HOST)
    distributed_training_context[_DistributedTrainingContextKey.HOSTS] = _os.environ.get(SM_ENV_VAR_HOSTS)
    distributed_training_context[_DistributedTrainingContextKey.NETWORK_INTERFACE_NAME] = _os.environ.get(
        SM_ENV_VAR_NETWORK_INTERFACE_NAME
    )

    return distributed_training_context


def get_sagemaker_distributed_training_context_from_file() -> dict:
    with open(SM_RESOURCE_CONFIG_FILE, "r") as rc_file:
        return _json.load(rc_file)
