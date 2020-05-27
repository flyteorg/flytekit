from __future__ import absolute_import

try:
    from inspect import getfullargspec as _getargspec
except ImportError:
    from inspect import getargspec as _getargspec

import six as _six
from flytekit.common import constants as _constants
from flytekit.common.exceptions import scopes as _exception_scopes
from flytekit.common.tasks import output as _task_output, sdk_runnable as _sdk_runnable
from flytekit.common.types import helpers as _type_helpers
from flytekit.models import literals as _literal_models, task as _task_models
from google.protobuf.json_format import MessageToDict as _MessageToDict


class SdkRunnablePytorchContainer(_sdk_runnable.SdkRunnableContainer):

    @property
    def args(self):
        """
        Override args to remove the injection of command prefixes
        :rtype: list[Text]
        """
        return self._args

class SdkPyTorchTask(_sdk_runnable.SdkRunnableTask):
    def __init__(
            self,
            task_function,
            task_type,
            discovery_version,
            retries,
            interruptible,
            deprecated,
            discoverable,
            timeout,
            workers_count,
            per_replica_storage_request,
            per_replica_cpu_request,
            per_replica_gpu_request,
            per_replica_memory_request,
            per_replica_storage_limit,
            per_replica_cpu_limit,
            per_replica_gpu_limit,
            per_replica_memory_limit,
            environment
    ):
        pytorch_job = _task_models.PyTorchJob(
            workers_count=workers_count
        ).to_flyte_idl()
        super(SdkPyTorchTask, self).__init__(
            task_function=task_function,
            task_type=task_type,
            discovery_version=discovery_version,
            retries=retries,
            interruptible=interruptible,
            deprecated=deprecated,
            storage_request=per_replica_storage_request,
            cpu_request=per_replica_cpu_request,
            gpu_request=per_replica_gpu_request,
            memory_request=per_replica_memory_request,
            storage_limit=per_replica_storage_limit,
            cpu_limit=per_replica_cpu_limit,
            gpu_limit=per_replica_gpu_limit,
            memory_limit=per_replica_memory_limit,
            discoverable=discoverable,
            timeout=timeout,
            environment=environment,
            custom=_MessageToDict(pytorch_job)
        )

    def _get_container_definition(
            self,
            **kwargs
    ):
        """
        :rtype: SdkRunnablePytorchContainer
        """
        return super(SdkPyTorchTask, self)._get_container_definition(cls=SdkRunnablePytorchContainer, **kwargs)
