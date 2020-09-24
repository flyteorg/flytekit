import json as _json
import os as _os

import retry as _retry

from flytekit.common import constants as _common_constants
from flytekit.common.constants import DistributedTrainingContextKey as _DistributedTrainingContextKey

SM_RESOURCE_CONFIG_FILE = "/opt/ml/input/config/resourceconfig.json"
SM_ENV_VAR_CURRENT_HOST = "SM_CURRENT_HOST"
SM_ENV_VAR_HOSTS = "SM_HOSTS"
SM_ENV_VAR_NETWORK_INTERFACE_NAME = "SM_NETWORK_INTERFACE_NAME"


@_retry.retry(exceptions=KeyError, delay=1, tries=10, backoff=1)
def get_sagemaker_distributed_training_context_from_env() -> dict:
    distributed_training_context = {}
    if (
        not _os.environ.get(SM_ENV_VAR_CURRENT_HOST)
        or not _os.environ.get(SM_ENV_VAR_HOSTS)
        or _os.environ.get(SM_ENV_VAR_NETWORK_INTERFACE_NAME)
    ):
        raise KeyError

    distributed_training_context[_DistributedTrainingContextKey.CURRENT_HOST] = _os.environ.get(SM_ENV_VAR_CURRENT_HOST)
    distributed_training_context[_DistributedTrainingContextKey.HOSTS] = _json.loads(_os.environ.get(SM_ENV_VAR_HOSTS))
    distributed_training_context[_DistributedTrainingContextKey.NETWORK_INTERFACE_NAME] = _os.environ.get(
        SM_ENV_VAR_NETWORK_INTERFACE_NAME
    )

    return distributed_training_context


@_retry.retry(exceptions=FileNotFoundError, delay=1, tries=10, backoff=1)
def get_sagemaker_distributed_training_context_from_file() -> dict:
    with open(SM_RESOURCE_CONFIG_FILE, "r") as rc_file:
        return _json.load(rc_file)


# The default output-persisting predicate.
# With this predicate, only the copy running on the first host in the list of hosts would persist its output
class DefaultOutputPersistPredicate(object):
    def __call__(self, distributed_training_context):
        return (
            distributed_training_context[_common_constants.DistributedTrainingContextKey.CURRENT_HOST]
            == distributed_training_context[_common_constants.DistributedTrainingContextKey.HOSTS][0]
        )
