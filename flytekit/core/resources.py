from dataclasses import dataclass, fields
from typing import TYPE_CHECKING, List, Optional, Union
from typing import Literal as L

from flyteidl.core import tasks_pb2

if TYPE_CHECKING:
    from kubernetes.client import V1PodSpec
from mashumaro.mixins.json import DataClassJSONMixin

from flytekit.core.constants import SHARED_MEMORY_MOUNT_NAME, SHARED_MEMORY_MOUNT_PATH
from flytekit.extras.accelerators import BaseAccelerator
from flytekit.models import task as task_models


@dataclass
class Resources(DataClassJSONMixin):
    """
    This class is used to specify both resource requests and resource limits.

    .. code-block:: python

        Resources(cpu="1", mem="2048")  # This is 1 CPU and 2 KB of memory
        Resources(cpu="100m", mem="2Gi")  # This is 1/10th of a CPU and 2 gigabytes of memory
        Resources(cpu=0.5, mem=1024) # This is 500m CPU and 1 KB of memory

        # For Kubernetes-based tasks, pods use ephemeral local storage for scratch space, caching, and for logs.
        # This allocates 1Gi of such local storage.
        Resources(ephemeral_storage="1Gi")

    .. note::

        Persistent storage is not currently supported on the Flyte backend.

    Please see the :std:ref:`User Guide <cookbook:customizing task resources>` for detailed examples.
    Also refer to the `K8s conventions. <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-units-in-kubernetes>`__
    """

    cpu: Optional[Union[str, int, float]] = None
    mem: Optional[Union[str, int]] = None
    gpu: Optional[Union[str, int]] = None
    ephemeral_storage: Optional[Union[str, int]] = None

    def __post_init__(self):
        def _check_cpu(value):
            if value is None:
                return
            if not isinstance(value, (str, int, float)):
                raise AssertionError(f"{value} should be of type str or int or float")

        def _check_others(value):
            if value is None:
                return
            if not isinstance(value, (str, int)):
                raise AssertionError(f"{value} should be of type str or int")

        _check_cpu(self.cpu)
        _check_others(self.mem)
        _check_others(self.gpu)
        _check_others(self.ephemeral_storage)


@dataclass
class ResourceSpec(DataClassJSONMixin):
    requests: Resources
    limits: Resources


_ResourceName = task_models.Resources.ResourceName
_ResourceEntry = task_models.Resources.ResourceEntry


def _convert_resources_to_resource_entries(resources: Resources) -> List[_ResourceEntry]:  # type: ignore
    resource_entries = []
    if resources.cpu is not None:
        resource_entries.append(_ResourceEntry(name=_ResourceName.CPU, value=str(resources.cpu)))
    if resources.mem is not None:
        resource_entries.append(_ResourceEntry(name=_ResourceName.MEMORY, value=str(resources.mem)))
    if resources.gpu is not None:
        resource_entries.append(_ResourceEntry(name=_ResourceName.GPU, value=str(resources.gpu)))
    if resources.ephemeral_storage is not None:
        resource_entries.append(
            _ResourceEntry(
                name=_ResourceName.EPHEMERAL_STORAGE,
                value=str(resources.ephemeral_storage),
            )
        )
    return resource_entries


def convert_resources_to_resource_model(
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
) -> task_models.Resources:
    """
    Convert flytekit ``Resources`` objects to a Resources model

    :param requests: Resource requests. Optional, defaults to ``None``
    :param limits: Resource limits. Optional, defaults to ``None``
    :return: The given resources as requests and limits
    """
    request_entries = []
    limit_entries = []
    if requests is not None:
        request_entries = _convert_resources_to_resource_entries(requests)
    if limits is not None:
        limit_entries = _convert_resources_to_resource_entries(limits)
    return task_models.Resources(requests=request_entries, limits=limit_entries)


def construct_extended_resources(
    *,
    accelerator: Optional[BaseAccelerator] = None,
    shared_memory: Optional[Union[L[True], str]] = None,
) -> Optional[tasks_pb2.ExtendedResources]:
    """Convert public extended resources to idl.

    :param accelerator: The accelerator to use for this task.
    :param shared_memory: If True, then shared memory will be attached to the container where the size is equal
        to the allocated memory. If str, then the shared memory is set to that size.
    """
    kwargs = {}
    if accelerator is not None:
        kwargs["gpu_accelerator"] = accelerator.to_flyte_idl()
    if isinstance(shared_memory, str) or shared_memory is True:
        if shared_memory is True:
            shared_memory = None
        kwargs["shared_memory"] = tasks_pb2.SharedMemory(
            mount_name=SHARED_MEMORY_MOUNT_NAME,
            mount_path=SHARED_MEMORY_MOUNT_PATH,
            size_limit=shared_memory,
        )

    if not kwargs:
        return None

    return tasks_pb2.ExtendedResources(**kwargs)


def pod_spec_from_resources(
    primary_container_name: Optional[str] = None,
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
    k8s_gpu_resource_key: str = "nvidia.com/gpu",
) -> "V1PodSpec":
    from kubernetes.client import V1Container, V1PodSpec, V1ResourceRequirements

    def _construct_k8s_pods_resources(resources: Optional[Resources], k8s_gpu_resource_key: str):
        if resources is None:
            return None

        resources_map = {
            "cpu": "cpu",
            "mem": "memory",
            "gpu": k8s_gpu_resource_key,
            "ephemeral_storage": "ephemeral-storage",
        }

        k8s_pod_resources = {}

        for resource in fields(resources):
            resource_value = getattr(resources, resource.name)
            if resource_value is not None:
                k8s_pod_resources[resources_map[resource.name]] = resource_value

        return k8s_pod_resources

    requests = _construct_k8s_pods_resources(resources=requests, k8s_gpu_resource_key=k8s_gpu_resource_key)
    limits = _construct_k8s_pods_resources(resources=limits, k8s_gpu_resource_key=k8s_gpu_resource_key)
    requests = requests or limits
    limits = limits or requests

    pod_spec = V1PodSpec(
        containers=[
            V1Container(
                name=primary_container_name,
                resources=V1ResourceRequirements(
                    requests=requests,
                    limits=limits,
                ),
            )
        ]
    )

    return pod_spec
