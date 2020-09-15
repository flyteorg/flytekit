from google.protobuf.json_format import MessageToDict as _MessageToDict

from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.models import task as _task_models


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
        environment,
    ):
        pytorch_job = _task_models.PyTorchJob(workers_count=workers_count).to_flyte_idl()
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
            custom=_MessageToDict(pytorch_job),
        )

    def _get_container_definition(self, **kwargs):
        """
        :rtype: SdkRunnablePytorchContainer
        """
        return super(SdkPyTorchTask, self)._get_container_definition(cls=SdkRunnablePytorchContainer, **kwargs)
