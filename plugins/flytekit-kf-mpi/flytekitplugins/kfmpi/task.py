"""
This Plugin adds the capability of running distributed MPI training to Flyte using backend plugins, natively on
Kubernetes. It leverages `MPI Job <https://github.com/kubeflow/mpi-operator>`_ Plugin from kubeflow.
"""
from dataclasses import dataclass
from typing import Any, Callable, Dict, List

from flyteidl.plugins import mpi_pb2 as _mpi_task
from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask
from flytekit.configuration import SerializationSettings
from flytekit.extend import TaskPlugins
from flytekit.models import common as _common


class MPIJobModel(_common.FlyteIdlEntity):
    """Model definition for MPI the plugin

    Args:
        num_workers: integer determining the number of worker replicas spawned in the cluster for this job
        (in addition to 1 master).

        num_launcher_replicas: Number of launcher server replicas to use

        slots: Number of slots per worker used in hostfile
        .. note::

            Please use resources=Resources(cpu="1"...) to specify per worker resource
    """

    def __init__(self, num_workers, num_launcher_replicas, slots):
        self._num_workers = num_workers
        self._num_launcher_replicas = num_launcher_replicas
        self._slots = slots

    @property
    def num_workers(self):
        return self._num_workers

    @property
    def num_launcher_replicas(self):
        return self._num_launcher_replicas

    @property
    def slots(self):
        return self._slots

    def to_flyte_idl(self):
        return _mpi_task.DistributedMPITrainingTask(
            num_workers=self.num_workers, num_launcher_replicas=self.num_launcher_replicas, slots=self.slots
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            num_workers=pb2_object.num_workers,
            num_launcher_replicas=pb2_object.num_launcher_replicas,
            slots=pb2_object.slots,
        )


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

    """

    slots: int
    num_launcher_replicas: int = 1
    num_workers: int = 1


class MPIFunctionTask(PythonFunctionTask[MPIJob]):
    """
    Plugin that submits a MPIJob (see https://github.com/kubeflow/mpi-operator)
        defined by the code within the _task_function to k8s cluster.
    """

    _MPI_JOB_TASK_TYPE = "mpi"
    _MPI_BASE_COMMAND = [
        "mpirun",
        "--allow-run-as-root",
        "-bind-to",
        "none",
        "-map-by",
        "slot",
        "-x",
        "LD_LIBRARY_PATH",
        "-x",
        "PATH",
        "-x",
        "NCCL_DEBUG=INFO",
        "-mca",
        "pml",
        "ob1",
        "-mca",
        "btl",
        "^openib",
    ]

    def __init__(self, task_config: MPIJob, task_function: Callable, **kwargs):
        super().__init__(
            task_config=task_config,
            task_function=task_function,
            task_type=self._MPI_JOB_TASK_TYPE,
            **kwargs,
        )

    def get_command(self, settings: SerializationSettings) -> List[str]:
        cmd = super().get_command(settings)
        num_procs = self.task_config.num_workers * self.task_config.slots
        mpi_cmd = self._MPI_BASE_COMMAND + ["-np", f"{num_procs}"] + ["python", settings.entrypoint_settings.path] + cmd
        # the hostfile is set automatically by MPIOperator using env variable OMPI_MCA_orte_default_hostfile
        return mpi_cmd

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        job = MPIJobModel(
            num_workers=self.task_config.num_workers,
            num_launcher_replicas=self.task_config.num_launcher_replicas,
            slots=self.task_config.slots,
        )
        return MessageToDict(job.to_flyte_idl())


# Register the MPI Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(MPIJob, MPIFunctionTask)
