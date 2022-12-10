from typing import List, Optional

import flytekit.models.task as _task_models
from flytekit import Resources

_ResouceName = _task_models.Resources.ResourceName
_ResourceEntry = _task_models.Resources.ResourceEntry


def _convert_resources_to_resouce_entries(resources: Resources) -> List[_ResourceEntry]:
    resource_entries = []
    if resources.cpu is not None:
        resource_entries.append(_ResourceEntry(name=_ResouceName.CPU, value=resources.cpu))
    if resources.mem is not None:
        resource_entries.append(_ResourceEntry(name=_ResouceName.MEMORY, value=resources.mem))
    if resources.gpu is not None:
        resource_entries.append(_ResourceEntry(name=_ResouceName.GPU, value=resources.gpu))
    if resources.storage is not None:
        resource_entries.append(_ResourceEntry(name=_ResouceName.STORAGE, value=resources.storage))
    if resources.ephemeral_storage is not None:
        resource_entries.append(_ResourceEntry(name=_ResouceName.EPHEMERAL_STORAGE, value=resources.ephemeral_storage))
    return resource_entries


def convert_resources_to_resource_model(
    requests: Optional[Resources] = None,
    limits: Optional[Resources] = None,
) -> _task_models.Resources:
    """
    Convert flytekit ``Resources`` objects to a Resources model

    :param requests: Resource requests. Optional, defaults to ``None``
    :param limits: Resource limits. Optional, defaults to ``None``
    :return: The given resources as requests and limits
    """
    request_entries = []
    limit_entries = []
    if requests is not None:
        request_entries = _convert_resources_to_resouce_entries(requests)
    if limits is not None:
        limit_entries = _convert_resources_to_resouce_entries(limits)
    return _task_models.Resources(requests=request_entries, limits=limit_entries)
