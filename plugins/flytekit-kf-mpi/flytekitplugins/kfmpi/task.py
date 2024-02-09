"""
This Plugin adds the capability of running distributed MPI training to Flyte using backend plugins, natively on
Kubernetes. It leverages `MPI Job <https://github.com/kubeflow/mpi-operator>`_ Plugin from kubeflow.
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

from flyteidl.plugins.kubeflow import common_pb2 as kubeflow_common
from flyteidl.plugins.kubeflow import mpi_pb2 as mpi_task
from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask, Resources
from flytekit.configuration import SerializationSettings
from flytekit.core.resources import convert_resources_to_resource_model
from flytekit.extend import TaskPlugins


@dataclass
class RestartPolicy(Enum):
    """
    RestartPolicy describes how the replicas should be restarted
    """

    ALWAYS = kubeflow_common.RESTART_POLICY_ALWAYS
    FAILURE = kubeflow_common.RESTART_POLICY_ON_FAILURE
    NEVER = kubeflow_common.RESTART_POLICY_NEVER


@dataclass
class CleanPodPolicy(Enum):
    """
    CleanPodPolicy describes how to deal with pods when the job is finished.
    """

    NONE = kubeflow_common.CLEANPOD_POLICY_NONE
    ALL = kubeflow_common.CLEANPOD_POLICY_ALL
    RUNNING = kubeflow_common.CLEANPOD_POLICY_RUNNING


@dataclass
class RunPolicy:
    """
    RunPolicy describes some policy to apply to the execution of a kubeflow job.
    Args:
        clean_pod_policy: Defines the policy for cleaning up pods after the PyTorchJob completes. Default to None.
        ttl_seconds_after_finished (int): Defines the TTL for cleaning up finished PyTorchJobs.
        active_deadline_seconds (int): Specifies the duration (in seconds) since startTime during which the job.
        can remain active before it is terminated. Must be a positive integer. This setting applies only to pods.
        where restartPolicy is OnFailure or Always.
        backoff_limit (int): Number of retries before marking this job as failed.
    """

    clean_pod_policy: CleanPodPolicy = None
    ttl_seconds_after_finished: Optional[int] = None
    active_deadline_seconds: Optional[int] = None
    backoff_limit: Optional[int] = None


@dataclass
class Worker:
    """
    Worker replica configuration. Worker command can be customized. If not specified, the worker will use
    default command generated by the mpi operator.
    """

    command: Optional[List[str]] = None
    image: Optional[str] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None
    replicas: Optional[int] = None
    restart_policy: Optional[RestartPolicy] = None
    node_selectors: Optional[Dict[str, str]] = None


@dataclass
class Launcher:
    """
    Launcher replica configuration. Launcher command can be customized. If not specified, the launcher will use
    the command specified in the task signature.
    """

    command: Optional[List[str]] = None
    image: Optional[str] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None
    replicas: Optional[int] = None
    restart_policy: Optional[RestartPolicy] = None
    node_selectors: Optional[Dict[str, str]] = None


@dataclass
class MPIJob(object):
    """
    Configuration for an executable `MPI Job <https://github.com/kubeflow/mpi-operator>`_. Use this
    to run distributed training on k8s with MPI

    Args:
        launcher: Configuration for the launcher replica group.
        worker: Configuration for the worker replica group.
        run_policy: Configuration for the run policy.
        slots: The number of slots per worker used in the hostfile.
        num_launcher_replicas: [DEPRECATED] The number of launcher server replicas to use. This argument is deprecated.
        num_workers: [DEPRECATED] The number of worker replicas to spawn in the cluster for this job
    """

    launcher: Launcher = field(default_factory=lambda: Launcher())
    worker: Worker = field(default_factory=lambda: Worker())
    run_policy: Optional[RunPolicy] = field(default_factory=lambda: None)
    slots: int = 1
    # Support v0 config for backwards compatibility
    num_launcher_replicas: Optional[int] = None
    num_workers: Optional[int] = None


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
        if task_config.num_workers and task_config.worker.replicas:
            raise ValueError(
                "Cannot specify both `num_workers` and `worker.replicas`. Please use `worker.replicas` as `num_workers` is depreacated."
            )
        if task_config.num_workers is None and task_config.worker.replicas is None:
            raise ValueError(
                "Must specify either `num_workers` or `worker.replicas`. Please use `worker.replicas` as `num_workers` is depreacated."
            )
        if task_config.num_launcher_replicas and task_config.launcher.replicas:
            raise ValueError(
                "Cannot specify both `num_workers` and `launcher.replicas`. Please use `launcher.replicas` as `num_launcher_replicas` is depreacated."
            )
        if task_config.num_launcher_replicas is None and task_config.launcher.replicas is None:
            raise ValueError(
                "Must specify either `num_workers` or `launcher.replicas`. Please use `launcher.replicas` as `num_launcher_replicas` is depreacated."
            )
        super().__init__(
            task_config=task_config,
            task_function=task_function,
            task_type=self._MPI_JOB_TASK_TYPE,
            # task_type_version controls the version of the task template, do not change
            task_type_version=1,
            **kwargs,
        )

    def _convert_replica_spec(
        self, replica_config: Union[Launcher, Worker]
    ) -> mpi_task.DistributedMPITrainingReplicaSpec:
        resources = convert_resources_to_resource_model(requests=replica_config.requests, limits=replica_config.limits)
        return mpi_task.DistributedMPITrainingReplicaSpec(
            command=replica_config.command,
            replicas=replica_config.replicas,
            image=replica_config.image,
            resources=resources.to_flyte_idl() if resources else None,
            restart_policy=replica_config.restart_policy.value if replica_config.restart_policy else None,
            node_selectors=replica_config.node_selectors,
        )

    def _convert_run_policy(self, run_policy: RunPolicy) -> kubeflow_common.RunPolicy:
        return kubeflow_common.RunPolicy(
            clean_pod_policy=run_policy.clean_pod_policy.value if run_policy.clean_pod_policy else None,
            ttl_seconds_after_finished=run_policy.ttl_seconds_after_finished,
            active_deadline_seconds=run_policy.active_deadline_seconds,
            backoff_limit=run_policy.backoff_limit,
        )

    def _get_base_command(self, settings: SerializationSettings) -> List[str]:
        return super().get_command(settings)

    def get_command(self, settings: SerializationSettings) -> List[str]:
        cmd = self._get_base_command(settings)
        if self.task_config.num_workers:
            num_workers = self.task_config.num_workers
        else:
            num_workers = self.task_config.worker.replicas
        num_procs = num_workers * self.task_config.slots
        mpi_cmd = self._MPI_BASE_COMMAND + ["-np", f"{num_procs}"] + ["python", settings.entrypoint_settings.path] + cmd
        # the hostfile is set automatically by MPIOperator using env variable OMPI_MCA_orte_default_hostfile
        return mpi_cmd

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        worker = self._convert_replica_spec(self.task_config.worker)
        if self.task_config.num_workers:
            worker.replicas = self.task_config.num_workers

        launcher = self._convert_replica_spec(self.task_config.launcher)
        if self.task_config.num_launcher_replicas:
            launcher.replicas = self.task_config.num_launcher_replicas

        run_policy = self._convert_run_policy(self.task_config.run_policy) if self.task_config.run_policy else None
        mpi_job = mpi_task.DistributedMPITrainingTask(
            worker_replicas=worker,
            launcher_replicas=launcher,
            slots=self.task_config.slots,
            run_policy=run_policy,
        )
        return MessageToDict(mpi_job)


@dataclass
class HorovodJob(object):
    """
    Configuration for an executable `Horovod Job using MPI operator<https://github.com/kubeflow/mpi-operator>`_. Use this
    to run distributed training on k8s with MPI. For more info, check out Running Horovod<https://horovod.readthedocs.io/en/stable/summary_include.html#running-horovod>`_.

    Args:
        worker: Worker configuration for the job.
        launcher: Launcher configuration for the job.
        run_policy: Configuration for the run policy.
        slots: Number of slots per worker used in hostfile (default: 1).
        verbose: Optional flag indicating whether to enable verbose logging (default: False).
        log_level: Optional string specifying the log level (default: "INFO").
        discovery_script_path: Path to the discovery script used for host discovery (default: "/etc/mpi/discover_hosts.sh").
        num_launcher_replicas: [DEPRECATED] The number of launcher server replicas to use. This argument is deprecated. Please use launcher.replicas instead.
        num_workers: [DEPRECATED] The number of worker replicas to spawn in the cluster for this job. Please use worker.replicas instead.
    """

    worker: Worker = field(default_factory=lambda: Worker())
    launcher: Launcher = field(default_factory=lambda: Launcher())
    run_policy: Optional[RunPolicy] = field(default_factory=lambda: None)
    slots: int = 1
    verbose: Optional[bool] = False
    log_level: Optional[str] = "INFO"
    discovery_script_path: Optional[str] = "/etc/mpi/discover_hosts.sh"
    # Support v0 config for backwards compatibility
    num_launcher_replicas: Optional[int] = None
    num_workers: Optional[int] = None


class HorovodFunctionTask(MPIFunctionTask):
    """
    For more info, check out https://github.com/horovod/horovod
    """

    # Customize your setup here. Please ensure the cmd, path, volume, etc are available in the pod.

    def __init__(self, task_config: HorovodJob, task_function: Callable, **kwargs):
        super().__init__(
            task_config=task_config,
            task_function=task_function,
            **kwargs,
        )

    def get_command(self, settings: SerializationSettings) -> List[str]:
        cmd = self._get_base_command(settings)
        mpi_cmd = self._get_horovod_prefix() + cmd
        return mpi_cmd

    def _get_horovod_prefix(self) -> List[str]:
        np = self.task_config.worker.replicas * self.task_config.slots
        log_level = self.task_config.log_level
        base_cmd = [
            "horovodrun",
            "-np",
            f"{np}",
            "--log-level",
            f"{log_level}",
            "--network-interface",
            "eth0",
            "--min-np",
            f"{np}",
            "--max-np",
            f"{np}",
            "--slots-per-host",
            f"{self.task_config.slots}",
            "--host-discovery-script",
            self.task_config.discovery_script_path,
        ]
        if self.task_config.verbose:
            base_cmd.append("--verbose")
        return base_cmd


# Register the MPI Plugin into the flytekit core plugin system
TaskPlugins.register_pythontask_plugin(MPIJob, MPIFunctionTask)
TaskPlugins.register_pythontask_plugin(HorovodJob, HorovodFunctionTask)
