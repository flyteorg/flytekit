from dataclasses import dataclass


@dataclass
class Resources(object):
    """
    This class is used to specify both resource requests and resource limits. ::

        Resources(cpu="1", mem="2048")  # This is 1 CPU and 2 KB of memory
        Resources(cpu="100m", mem="2Gi")  # This is 1/10th of a CPU and 2 gigabytes of memory

    .. note::
        Storage is not currently supported on the Flyte backend.

    Please see :std:doc:`cookbook:auto_core_remote_flyte/customizing_resources` for detailed examples.
    Also refer to the `K8s conventions. <https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#resource-units-in-kubernetes>`__
    """

    cpu: str = None
    mem: str = None
    gpu: str = None
    storage: str = None


@dataclass
class ResourceSpec(object):
    requests: Resources = None
    limits: Resources = None
