from typing import Optional

from flyteidl.plugins import dask_pb2 as _dask_task

from flytekit.models import common as _common
from flytekit.models import task as _task


class JobPodSpec(_common.FlyteIdlEntity):
    """
    Configuration for the job runner pod

    :param image: Optional image to use.
    :param resources: Optional resources to use.
    """

    def __init__(self, image: Optional[str], resources: Optional[_task.Resources]):
        self._image = image
        self._resources = resources

    @property
    def image(self) -> Optional[str]:
        """
        :return: The optional image for the job runner pod
        """
        return self._image

    @property
    def resources(self) -> Optional[_task.Resources]:
        """
        :return: Optional resources for the job runner pod
        """
        return self._resources

    def to_flyte_idl(self) -> _dask_task.JobPodSpec:
        """
        :return: The job pod spec serialized to protobuf
        """
        return _dask_task.JobPodSpec(
            image=self.image,
            resources=self.resources.to_flyte_idl() if self.resources else None,
        )


class DaskCluster(_common.FlyteIdlEntity):
    """
    Configuration for the dask cluster the job runner connects to

    :param image: Optional image to use for the cluster pods (scheduler and workers)
    :param n_workers: Optional worker count to use for the dask cluster
    :param resources: Optional resources to use for the cluster pods (scheduler and workers)
    """

    def __init__(self, image: Optional[str], n_workers: Optional[int], resources: Optional[_task.Resources]):
        self._image = image
        self._n_workers = n_workers
        self._resources = resources

    @property
    def image(self) -> Optional[str]:
        """
        :return: The optional image to use for the cluster pods
        """
        return self._image

    @property
    def n_workers(self) -> Optional[int]:
        """
        :return: Optional number of workers for the cluster
        """
        return self._n_workers

    @property
    def resources(self) -> Optional[_task.Resources]:
        """
        :return: Optional resources for the pods of the cluster
        """
        return self._resources

    def to_flyte_idl(self) -> _dask_task.DaskCluster:
        """
        :return: The dask cluster serialized to protobuf
        """
        return _dask_task.DaskCluster(
            image=self.image,
            nWorkers=self.n_workers,
            resources=self.resources.to_flyte_idl() if self.resources else None,
        )


class DaskJob(_common.FlyteIdlEntity):
    """
    Configuration for the custom dask job to run

    :param job_pod_spec: Configuration for the job runner pod
    :param dask_cluster: Configuration for the dask cluster
    """

    def __init__(self, job_pod_spec: JobPodSpec, dask_cluster: DaskCluster):
        self._job_pod_spec = job_pod_spec
        self._dask_cluster = dask_cluster

    @property
    def job_pod_spec(self) -> JobPodSpec:
        """
        :return: Configuration for the job runner pod
        """
        return self._job_pod_spec

    @property
    def dask_cluster(self) -> DaskCluster:
        """
        :return: Configuration for the dask cluster
        """
        return self._dask_cluster

    def to_flyte_idl(self) -> _dask_task.DaskJob:
        """
        :return: The dask job serialized to protobuf
        """
        return _dask_task.DaskJob(
            jobPodSpec=self.job_pod_spec.to_flyte_idl(),
            cluster=self.dask_cluster.to_flyte_idl(),
        )
