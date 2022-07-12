import typing

from flyteidl.core import resource_pb2 as _resource_pb2

from flytekit.models import common as _common


class WorkerGroupSpec(_common.FlyteIdlEntity):
    def __init__(
        self,
        group_name: str,
        replicas: int,
        min_replicas: typing.Optional[int] = 0,
        max_replicas: typing.Optional[int] = None,
        compute_template: typing.Optional[str] = None,
        image: typing.Optional[str] = "rayproject/ray:1.8.0",
        ray_start_params: typing.Optional[typing.Dict[str, str]] = None,
    ):
        self._group_name = group_name
        self._compute_template = compute_template
        self._image = image
        self._replicas = replicas
        self._min_replicas = min_replicas
        self._max_replicas = max_replicas if max_replicas else replicas
        self._ray_start_params = ray_start_params

    @property
    def group_name(self):
        """
        Group name of the current worker group.
        :rtype: str
        """
        return self._group_name

    @property
    def compute_template(self):
        """
        The computeTemplate of head node group.
        :rtype: str
        """
        return self._compute_template

    @property
    def image(self):
        """
        This field will be used to retrieve right ray container.
        :rtype: str
        """
        return self._image

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
        :rtype: flyteidl.core._resource_pb2.WorkerGroupSpec
        """
        return _resource_pb2.WorkerGroupSpec(
            group_name=self.group_name,
            compute_template=self.compute_template,
            image=self.image,
            replicas=self.replicas,
            min_replicas=self.min_replicas,
            max_replicas=self.max_replicas,
            ray_start_params=self.ray_start_params,
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core._resource_pb2.WorkerGroupSpec proto:
        :rtype: WorkerGroupSpec
        """
        return cls(
            group_name=proto.group_name,
            compute_template=proto.compute_template,
            image=proto.image,
            replicas=proto.replicas,
            min_replicas=proto.min_replicas,
            max_replicas=proto.max_replicas,
            ray_start_params=proto.ray_start_params,
        )


class HeadGroupSpec(_common.FlyteIdlEntity):
    def __init__(
        self,
        compute_template: typing.Optional[str] = None,
        image: typing.Optional[str] = "rayproject/ray:1.8.0",
        ray_start_params: typing.Optional[typing.Dict[str, str]] = None,
        service_type: typing.Optional[str] = "ClusterIP",
    ):
        self._compute_template = compute_template
        self._image = image
        self._ray_start_params = ray_start_params
        self._service_type = service_type

    @property
    def compute_template(self):
        """
        The computeTemplate of head node group.
        :rtype: str
        """
        return self._compute_template

    @property
    def image(self):
        """
        This field will be used to retrieve right ray container.
        :rtype: str
        """
        return self._image

    @property
    def ray_start_params(self):
        """
        The ray start params of worker node group.
        :rtype: typing.Dict[str, str]
        """
        return self._ray_start_params

    @property
    def service_type(self):
        """
        The service type (ClusterIP, NodePort, Load balancer) of the head node.
        :rtype: str
        """
        return self._service_type

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core._resource_pb2.HeadGroupSpec
        """
        return _resource_pb2.HeadGroupSpec(
            compute_template=self.compute_template,
            image=self.image,
            service_type=self.service_type,
            ray_start_params=self.ray_start_params if self.ray_start_params else {},
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core._resource_pb2.HeadGroupSpec proto:
        :rtype: HeadGroupSpec
        """
        return cls(
            compute_template=proto.compute_template,
            image=proto.image,
            service_type=proto.service_type,
            ray_start_params=proto.ray_start_params,
        )


class ClusterSpec(_common.FlyteIdlEntity):
    def __init__(
        self,
        worker_group_spec: typing.List[WorkerGroupSpec],
        head_group_spec: typing.Optional[HeadGroupSpec] = HeadGroupSpec(),
    ):
        self._head_group_spec = head_group_spec
        self._worker_group_spec = worker_group_spec

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

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core._resource_pb2.ClusterSpec
        """
        return _resource_pb2.ClusterSpec(
            head_group_spec=self.head_group_spec.to_flyte_idl(),
            worker_group_spec=[wg.to_flyte_idl() for wg in self.worker_group_spec] if self.worker_group_spec else None,
        )

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core._resource_pb2.ClusterSpec proto:
        :rtype: ClusterSpec
        """
        return cls(
            head_group_spec=HeadGroupSpec.from_flyte_idl(proto.head_group_spec) if proto.head_group_spec else None,
            worker_group_spec=[WorkerGroupSpec.from_flyte_idl(wg) for wg in proto.worker_group_spec]
            if proto.worker_group_spec
            else None,
        )


class RayCluster(_common.FlyteIdlEntity):
    """
    Define RayCluster spec that will be used by KubeRay to launch the cluster.
    """

    def __init__(self, name: str, cluster_spec: ClusterSpec):
        self._name = name
        self._cluster_spec = cluster_spec

    @property
    def name(self):
        """
        Unique cluster name provided by user.
        :rtype: str
        """
        return self._name

    @property
    def cluster_spec(self):
        """
        This field indicates ray cluster configuration
        :rtype: ClusterSpec
        """
        return self._cluster_spec

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core._resource_pb2.RayCluster
        """
        return _resource_pb2.RayCluster(name=self.name, cluster_spec=self.cluster_spec.to_flyte_idl())

    @classmethod
    def from_flyte_idl(cls, proto):
        """
        :param flyteidl.core._resource_pb2.RayCluster proto:
        :rtype: RayCluster
        """
        return cls(name=proto.name, cluster_spec=ClusterSpec.from_flyte_idl(proto.cluster_spec))


class Resource(_common.FlyteIdlEntity):
    """
    Models _resource_pb2.Resource
    """

    def __init__(self, ray: typing.Optional[RayCluster]):
        self._ray = ray

    @property
    def ray(self) -> typing.Optional[RayCluster]:
        return self._ray

    def to_flyte_idl(self) -> _resource_pb2.Resource:
        return _resource_pb2.Resource(ray=self.ray.to_flyte_idl() if self.ray else None)

    @classmethod
    def from_flyte_idl(cls, proto: _resource_pb2.Resource):
        return cls(ray=RayCluster.from_flyte_idl(proto.ray) if proto.ray else None)
