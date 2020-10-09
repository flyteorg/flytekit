import json as _json
import os as _os

import retry as _retry

from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.engines import common as _common_engine

SM_RESOURCE_CONFIG_FILE = "/opt/ml/input/config/resourceconfig.json"
SM_ENV_VAR_CURRENT_HOST = "SM_CURRENT_HOST"
SM_ENV_VAR_HOSTS = "SM_HOSTS"
SM_ENV_VAR_NETWORK_INTERFACE_NAME = "SM_NETWORK_INTERFACE_NAME"


# SageMaker suggests "Hostname information might not be immediately available to the processing container.
# We recommend adding a retry policy on hostname resolution operations as nodes become available in the cluster."
# https://docs.aws.amazon.com/sagemaker/latest/dg/build-your-own-processing-container.html#byoc-config
@_retry.retry(exceptions=KeyError, delay=1, tries=10, backoff=1)
def get_sagemaker_distributed_training_context_from_env() -> dict:
    distributed_training_context = {}
    if (
        not _os.environ.get(SM_ENV_VAR_CURRENT_HOST)
        or not _os.environ.get(SM_ENV_VAR_HOSTS)
        or not _os.environ.get(SM_ENV_VAR_NETWORK_INTERFACE_NAME)
    ):
        raise KeyError

    distributed_training_context[DistributedTrainingContextKey.CURRENT_HOST] = _os.environ.get(SM_ENV_VAR_CURRENT_HOST)
    distributed_training_context[DistributedTrainingContextKey.HOSTS] = _json.loads(_os.environ.get(SM_ENV_VAR_HOSTS))
    distributed_training_context[DistributedTrainingContextKey.NETWORK_INTERFACE_NAME] = _os.environ.get(
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
            distributed_training_context[DistributedTrainingContextKey.CURRENT_HOST]
            == distributed_training_context[DistributedTrainingContextKey.HOSTS][0]
        )


class DistributedTrainingContextKey(object):
    CURRENT_HOST = "current_host"
    HOSTS = "hosts"
    NETWORK_INTERFACE_NAME = "network_interface_name"


class DistributedTrainingEngineContext(_common_engine.EngineContext):
    def __init__(
        self,
        execution_date,
        tmp_dir,
        stats,
        execution_id,
        logging,
        raw_output_data_prefix=None,
        distributed_training_context=None,
    ):
        super().__init__(
            execution_date=execution_date,
            tmp_dir=tmp_dir,
            stats=stats,
            execution_id=execution_id,
            logging=logging,
            raw_output_data_prefix=raw_output_data_prefix,
        )
        self._distributed_training_context = distributed_training_context

    @property
    def distributed_training_context(self) -> dict:
        return self._distributed_training_context


class DistributedTrainingExecutionParam(_sdk_runnable.ExecutionParameters):
    def __init__(self, execution_date, tmp_dir, stats, execution_id, logging, distributed_training_context):

        super().__init__(
            execution_date=execution_date, tmp_dir=tmp_dir, stats=stats, execution_id=execution_id, logging=logging
        )

        self._distributed_training_context = distributed_training_context

    @property
    def distributed_training_context(self):
        """
        This contains the resource information for distributed training. Currently this information is only available
        for SageMaker training jobs.

        :rtype: dict
        """
        return self._distributed_training_context
