import typing

from flyteidl.plugins import ray_pb2 as _ray_pb2

from flytekit.models import common as _common


class WorkerGroupSpec(_common.FlyteIdlEntity):
    def __init__(
        self,
        group_name: str,
        replicas: int,
        min_replicas: typing.Optional[int] = None,
        max_replicas: typing.Optional[int] = None,
        ray_start_params: typing.Optional[typing.Dict[str, str]] = None,
    ):
        self._group_name = group_name
        self._replicas = replicas
        self._max_replicas = max(replicas, max_replicas) if max_replicas is not None else replicas
        self._min_replicas = min(replicas, min_replicas) if min_replicas is not None else replicas
        self._ray_start_params = ray_start_params

    @property
    def group_name(self):
        """
        Group name of the current worker group.
        :rtype: str
        """
        return self._group_name

    @property
    def replicas(self):
        """
        Desired replicas of the worker group.
        :rtype: int
        """
        return self._replicas

    @property
    def min_replicas(self):
        """
        Min replicas of the worker group.
        :rtype: int
        """
        return self._min_replicas

    @property
    def max_replicas(self):
        """
        Max replicas of the worker group.
        :rtype: int
        """
        return self._max_replicas

    @property
    def ray_start_params(self):
        """
        The ray start params of worker node group.
        :rtype: typing.Dict[str, str]
        """
        return self._ray_start_params

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.plugins._ray_pb2.WorkerGroupSpec
        """
        return _ray_pb2.WorkerGroupSpec(
            group_name=self.group_name,
            replicas=self.replicas,
            min_replicas=self.min_replicas,
            max_replicas=self.max_replicas,
            ray_start_params=self.ray_start_params,
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.plugins._ray_pb2.WorkerGroupSpec proto:
        :rtype: WorkerGroupSpec
        """
        return cls(
            group_name=proto.group_name,
            replicas=proto.replicas,
            min_replicas=proto.min_replicas,
            max_replicas=proto.max_replicas,
            ray_start_params=proto.ray_start_params,
        )


class HeadGroupSpec(_common.FlyteIdlEntity):
    def __init__(
        self,
        ray_start_params: typing.Optional[typing.Dict[str, str]] = None,
    ):
        self._ray_start_params = ray_start_params

    @property
    def ray_start_params(self):
        """
        The ray start params of worker node group.
        :rtype: typing.Dict[str, str]
        """
        return self._ray_start_params

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.plugins._ray_pb2.HeadGroupSpec
        """
        return _ray_pb2.HeadGroupSpec(
            ray_start_params=self.ray_start_params if self.ray_start_params else {},
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.plugins._ray_pb2.HeadGroupSpec proto:
        :rtype: HeadGroupSpec
        """
        return cls(
            ray_start_params=proto.ray_start_params,
        )


class RayCluster(_common.FlyteIdlEntity):
    """
    Define RayCluster spec that will be used by KubeRay to launch the cluster.
    """

    def __init__(
        self,
        worker_group_spec: typing.List[WorkerGroupSpec],
        head_group_spec: typing.Optional[HeadGroupSpec] = None,
        enable_autoscaling: bool = False,
    ):
        self._head_group_spec = head_group_spec
        self._worker_group_spec = worker_group_spec
        self._enable_autoscaling = enable_autoscaling

    @property
    def head_group_spec(self) -> HeadGroupSpec:
        """
        The head group configuration.
        :rtype: HeadGroupSpec
        """
        return self._head_group_spec

    @property
    def worker_group_spec(self) -> typing.List[WorkerGroupSpec]:
        """
        The worker group configurations.
        :rtype: typing.List[WorkerGroupSpec]
        """
        return self._worker_group_spec

    @property
    def enable_autoscaling(self) -> bool:
        """
        Whether to enable autoscaling.
        :rtype: bool
        """
        return self._enable_autoscaling

    def to_flyte_idl(self) -> _ray_pb2.RayCluster:
        """
        :rtype: flyteidl.plugins._ray_pb2.RayCluster
        """
        return _ray_pb2.RayCluster(
            head_group_spec=self.head_group_spec.to_flyte_idl() if self.head_group_spec else None,
            worker_group_spec=[wg.to_flyte_idl() for wg in self.worker_group_spec],
            enable_autoscaling=self.enable_autoscaling,
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.plugins._ray_pb2.RayCluster proto:
        :rtype: RayCluster
        """
        return cls(
            head_group_spec=HeadGroupSpec.from_flyte_idl(proto.head_group_spec) if proto.head_group_spec else None,
            worker_group_spec=[WorkerGroupSpec.from_flyte_idl(wg) for wg in proto.worker_group_spec],
            enable_autoscaling=proto.enable_autoscaling,
        )


class RayJob(_common.FlyteIdlEntity):
    """
    Models _ray_pb2.RayJob
    """

    def __init__(
        self,
        ray_cluster: RayCluster,
        runtime_env: typing.Optional[str] = None,
        runtime_env_yaml: typing.Optional[str] = None,
        ttl_seconds_after_finished: typing.Optional[int] = None,
        shutdown_after_job_finishes: bool = False,
    ):
        self._ray_cluster = ray_cluster
        self._runtime_env = runtime_env
        self._runtime_env_yaml = runtime_env_yaml
        self._ttl_seconds_after_finished = ttl_seconds_after_finished
        self._shutdown_after_job_finishes = shutdown_after_job_finishes

    @property
    def ray_cluster(self) -> RayCluster:
        return self._ray_cluster

    @property
    def runtime_env(self) -> typing.Optional[str]:
        return self._runtime_env

    @property
    def runtime_env_yaml(self) -> typing.Optional[str]:
        return self._runtime_env_yaml

    @property
    def ttl_seconds_after_finished(self) -> typing.Optional[int]:
        # ttl_seconds_after_finished specifies the number of seconds after which the RayCluster will be deleted after the RayJob finishes.
        return self._ttl_seconds_after_finished

    @property
    def shutdown_after_job_finishes(self) -> bool:
        # shutdown_after_job_finishes specifies whether the RayCluster should be deleted after the RayJob finishes.
        return self._shutdown_after_job_finishes

    def to_flyte_idl(self) -> _ray_pb2.RayJob:
        return _ray_pb2.RayJob(
            ray_cluster=self.ray_cluster.to_flyte_idl(),
            runtime_env=self.runtime_env,
            runtime_env_yaml=self.runtime_env_yaml,
            ttl_seconds_after_finished=self.ttl_seconds_after_finished,
            shutdown_after_job_finishes=self.shutdown_after_job_finishes,
        )

    @classmethod
    def from_flyte_idl(cls, proto: _ray_pb2.RayJob):
        return cls(
            ray_cluster=RayCluster.from_flyte_idl(proto.ray_cluster) if proto.ray_cluster else None,
            runtime_env=proto.runtime_env,
            runtime_env_yaml=proto.runtime_env_yaml,
            ttl_seconds_after_finished=proto.ttl_seconds_after_finished,
            shutdown_after_job_finishes=proto.shutdown_after_job_finishes,
        )
