"""
This Plugin adds the capability of running distributed tensorflow training to Flyte using backend plugins, natively on
Kubernetes. It leverages `TF Job <https://github.com/kubeflow/mpi-operator>`_ Plugin from kubeflow.
"""
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask, Resources
from flytekit.extend import SerializationSettings, TaskPlugins
from flytekit.models import task as _task_model


@dataclass
class MPIJob(object):
    """
    Configuration for an executable `MPI Job <https://github.com/kubeflow/mpi-operator>`_. Use this
    to run distributed training on k8s with MPI

    Args:
        num_workers: integer determining the number of worker replicas spawned in the cluster for this job
        (in addition to 1 master).

        num_launcher_replicas: Number of launcher server replicas to use

        slots: Number of slots per worker used in hostfile

        per_replica_requests: [optional] lower-bound resources for each replica spawned for this job
        (i.e. both for (main)master and workers).  Default is set by platform-level configuration.

        per_replica_limits: [optional] upper-bound resources for each replica spawned for this job. If not specified
        the scheduled resource may not have all the resources
    """

    num_workers: int
    num_launcher_replicas: int
    slots: int
    per_replica_requests: Optional[Resources] = None
    per_replica_limits: Optional[Resources] = None


class MPIFunctionTask(PythonFunctionTask[MPIJob]):
    """
    Plugin that submits a MPIJob (see https://github.com/kubeflow/mpi-operator)
        defined by the code within the _task_function to k8s cluster.
    """

    _MPI_JOB_TASK_TYPE = "mpi"

    def __init__(self, task_config: MPIJob, task_function: Callable, **kwargs):
        super().__init__(
            task_type=self._MPI_JOB_TASK_TYPE,
            task_config=task_config,
            task_function=task_function,
            **{**kwargs, "requests": task_config.per_replica_requests, "limits": task_config.per_replica_limits}
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:

        job = _task_model.MPIJob(
            workers_count=self.task_config.num_workers,
            num_launcher_replicas=self.task_config.num_launcher_replicas,
            slots=self.task_config.slots,
        )
        return MessageToDict(job.to_flyte_idl())


# Register the MPI Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(MPIJob, MPIFunctionTask)
