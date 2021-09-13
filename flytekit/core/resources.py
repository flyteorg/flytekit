from dataclasses import dataclass
from typing import Optional


@dataclass
class Resources(object):
    """
    This class is used to specify both resource requests and resource limits.

    .. code-block:: python

        Resources(cpu="1", mem="2048")  # This is 1 CPU and 2 KB of memory
        Resources(cpu="100m", mem="2Gi")  # This is 1/10th of a CPU and 2 gigabytes of memory

        # For Kubernetes-based tasks, pods use ephemeral local storage for scratch space, caching, and for logs.
        # This allocates 1Gi of such local storage.
        Resources(ephemeral_storage="1Gi")

    .. note::

        Persistent storage is not currently supported on the Flyte backend.

    Please see the :std:ref:`User Guide <cookbook:customizing task resources>` for detailed examples.
    Also refer to the `K8s conventions. <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-units-in-kubernetes>`__
    """

    cpu: Optional[str] = None
    mem: Optional[str] = None
    gpu: Optional[str] = None
    storage: Optional[str] = None
    ephemeral_storage: Optional[str] = None


@dataclass
class ResourceSpec(object):
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None
