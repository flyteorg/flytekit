from google.protobuf.json_format import MessageToDict as _MessageToDict

from flytekit.common.tasks import sdk_runnable as _sdk_runnable
from flytekit.models import task as _task_models


class SdkRunnableTensorflowContainer(_sdk_runnable.SdkRunnableContainer):
    @property
    def args(self):
        """
        Override args to remove the injection of command prefixes
        :rtype: list[Text]
        """
        return self._args


class SdkTensorFlowTask(_sdk_runnable.SdkRunnableTask):
    def __init__(
        self,
        task_function,
        task_type,
        cache_version,
        retries,
        interruptible,
        deprecated,
        cache,
        timeout,
        workers_count,
        ps_replicas_count,
        chief_replicas_count,
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
        tensorflow_job = _task_models.TensorFlowJob(
            workers_count=workers_count, ps_replicas_count=ps_replicas_count, chief_replicas_count=chief_replicas_count
        ).to_flyte_idl()
        super(SdkTensorFlowTask, self).__init__(
            task_function=task_function,
            task_type=task_type,
            discovery_version=cache_version,
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
            discoverable=cache,
            timeout=timeout,
            environment=environment,
            custom=_MessageToDict(tensorflow_job),
        )

    def _get_container_definition(self, **kwargs):
        """
        :rtype: SdkRunnableTensorflowContainer
        """
        return super(SdkTensorFlowTask, self)._get_container_definition(cls=SdkRunnableTensorflowContainer, **kwargs)
