from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from flytekitplugins.dask import models
from flytekitplugins.dask.resources import convert_resources_to_resource_model
from google.protobuf.json_format import MessageToDict

from flytekit import PythonFunctionTask, Resources
from flytekit.configuration import SerializationSettings
from flytekit.core.task import TaskPlugins


@dataclass
class JobPodSpec:
    """
    Configuration for the dask job runner pod

    :param image: Custom image to use. If ``None``, will use the same image the task was registered with. Optional,
        defaults to ``None``. The image must have ``dask[distributed]`` installed and should have the same Python
        environment as the cluster (scheduler + worker pods).
    :param requests: Resources to request for the job runner pod. If ``None``, the requests passed into the task will be
        used. Optional, defaults to ``None``
    :param limits: Resource limits for the job runner pod. If ``None``, the limits passed into the task will be used.
        Optional, defaults to ``None``
    """

    image: Optional[str] = None
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None


@dataclass
class DaskCluster:
    """
    Configuration for the dask cluster pods

    :param image: Custom image to use. If ``None``, will use the same image the task was registered with. Optional,
        defaults to ``None``. The image must have ``dask[distributed]`` installed. The provided image should have the
        same Python environment as the job runner/driver.
    :param n_workers: Number of workers to use. Optional, defaults to 1.
    :param requests: Resources to request for the scheduler pod as well as the worker pods. If ``None``, the requests
        passed into the task will be used. Optional, defaults to ``None``
    :param limits: Resource limits for the scheduler pod as well as the worker pods. If ``None``, the limits
        passed into the task will be used. Optional, defaults to ``None``
    """

    image: Optional[str] = None
    n_workers: Optional[int] = 1
    requests: Optional[Resources] = None
    limits: Optional[Resources] = None


@dataclass
class Dask:
    """
    Configuration for the dask task

    :param job_pod_spec: Configuration for the job runner pod. Optional, defaults to ``JobPodSpec()``
    :param cluster: Configuration for the dask cluster pods (scheduler and workers). Optional, defaults to
        ``DaskCluster()``
    """

    job_pod_spec: JobPodSpec = JobPodSpec()
    cluster: DaskCluster = DaskCluster()


class DaskTask(PythonFunctionTask[Dask]):
    """
    Actual Plugin that transforms the local python code for execution within a dask cluster
    """

    _DASK_TASK_TYPE = "dask"

    def __init__(self, task_config: Dask, task_function: Callable, **kwargs):
        super(DaskTask, self).__init__(
            task_config=task_config,
            task_type=self._DASK_TASK_TYPE,
            task_function=task_function,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Optional[Dict[str, Any]]:
        """
        Serialize the `dask` task config into a dict.

        :param settings: Current serialization settings
        :return: Dictionary representation of the dask task config.
        """
        job_pod_spec = models.JobPodSpec(
            image=self.task_config.job_pod_spec.image,
            resources=convert_resources_to_resource_model(
                requests=self.task_config.job_pod_spec.requests,
                limits=self.task_config.job_pod_spec.limits,
            ),
        )
        dask_cluster = models.DaskCluster(
            image=self.task_config.cluster.image,
            n_workers=self.task_config.cluster.n_workers,
            resources=convert_resources_to_resource_model(
                requests=self.task_config.cluster.requests,
                limits=self.task_config.cluster.limits,
            ),
        )
        job = models.DaskJob(job_pod_spec=job_pod_spec, dask_cluster=dask_cluster)
        return MessageToDict(job.to_flyte_idl())


# Inject the `dask` plugin into flytekits dynamic plugin loading system
TaskPlugins.register_pythontask_plugin(Dask, DaskTask)
