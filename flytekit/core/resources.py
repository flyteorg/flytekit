from dataclasses import dataclass, fields
from typing import Any, List, Optional, Union

from kubernetes.client import V1Container, V1PodSpec, V1ResourceRequirements
from mashumaro.mixins.json import DataClassJSONMixin

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


def pod_spec_from_resources(
    k8s_pod_name: str,
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
    k8s_gpu_resource_key: str = "nvidia.com/gpu",
) -> dict[str, Any]:
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

    k8s_pod = V1PodSpec(
        containers=[
            V1Container(
                name=k8s_pod_name,
                resources=V1ResourceRequirements(
                    requests=requests,
                    limits=limits,
                ),
            )
        ]
    )

    return k8s_pod.to_dict()
