from dataclasses import dataclass


@dataclass
class Resource(object):
    cpu: str = None
    mem: str = None
    gpu: str = None
    storage: str = None


@dataclass
class ResourceSpec(object):
    requests: Resource = None
    limits: Resource = None


def get_resources(
    memory_request=None,
    memory_limit=None,
    cpu_request=None,
    cpu_limit=None,
    storage_request=None,
    storage_limit=None,
    gpu_request=None,
    gpu_limit=None,
    **kwargs
) -> ResourceSpec:
    resources = ResourceSpec()
    resources.requests = Resource()
    resources.limits = Resource()
    if memory_request:
        resources.requests.mem = memory_request
    if memory_limit:
        resources.limits.mem = memory_limit
    if cpu_request:
        resources.requests.cpu = cpu_request
    if cpu_limit:
        resources.limits.cpu = cpu_limit
    if storage_request:
        resources.requests.storage = storage_request
    if storage_limit:
        resources.limits.storage = storage_limit
    if gpu_request:
        resources.requests.gpu = gpu_request
    if gpu_limit:
        resources.limits.gpu = gpu_limit
    return resources
