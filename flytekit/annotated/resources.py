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


def _get_resources(**kwargs):
    resources = ResourceSpec()
    resources.requests = Resource()
    resources.limits = Resource()
    for key, value in kwargs.items():
        if key == "memory_request":
            resources.requests.mem = value
        elif key == "memory_limit":
            resources.limits.mem = value
        elif key == "cpu_request":
            resources.requests.cpu = value
        elif key == "cpu_limit":
            resources.limits.cpu = value
        elif key == "storage_request":
            resources.requests.storage = value
        elif key == "storage_limit":
            resources.limits.storage = value
        elif key == "gpu_request":
            resources.requests.gpu = value
        elif key == "gpu_limit":
            resources.limits.gpu = value
    return resources
