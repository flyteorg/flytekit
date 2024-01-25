import typing

from flyteidl.core import execution_envs_pb2 as _execution_envs_pb2

from flytekit.models import common as _common_models

class FastTaskEnvironment(_common_models.FlyteIdlEntity):
    def __init__(
        self,
        queue_id: str,
        namespace: str,
        pod_name: str,
    ):
        """
        :param type: TODO @hamersaw
        :param namespace: TODO @hamersaw
        :param podName: TODO @hamersaw
        """
        self._queue_id = queue_id
        self._namespace = namespace
        self._pod_name = pod_name

    @property
    def queue_id(self) -> str:
        """
        TODO @hamersaw
        """
        return self._queue_id

    @property
    def namespace(self) -> str:
        """
        TODO @hamersaw
        """
        return self._namespace

    @property
    def pod_name(self) -> str:
        """
        TODO @hamersaw
        """
        return self._pod_name

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_envs_pb2.Environment
        """
        return _execution_envs_pb2.FastTaskEnvironment(
            queue_id=self.queue_id,
            namespace=self.namespace,
            pod_name=self.pod_name,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param TODO @hamersaw pb2_object:
        :return: TODO @hamersaw
        """
        return cls(
            queue_id=pb2_object.queue_id,
            namespace=pb2_object.namespace,
            pod_name=pb2_object.pod_name,
        )

class Environment(_common_models.FlyteIdlEntity):
    #class EnvironmentType(object):
    #    FAST_TASK = 0

    def __init__(
        self,
        type: int,
        fast_task: typing.Optional[FastTaskEnvironment] = None,
    ):
        """
        :param type: TODO @hamersaw
        :param fast_task: Optional, TODO @hamersaw
        """
        self._type = type
        self._fast_task = fast_task

    @property
    def type(self) -> int:
        """
        TODO @hamersaw
        """
        return self._type

    @property
    def fast_task(self) -> FastTaskEnvironment:
        """
        TODO @hamersaw
        """
        return self._fast_task

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_envs_pb2.ExecutionEnvironment
        """
        return _execution_envs_pb2.ExecutionEnvironment(
            type=self.type,
            fast_task=self.fast_task.to_flyte_idl()
            if self.fast_task is not None
            else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param TODO @hamersaw pb2_object:
        :return: TODO @hamersaw
        """
        return cls(
            type=pb2_object.type,
            fast_task=FastTaskEnvironment.from_flyte_idl(pb2_object.fast_task)
            if pb2_object.HasField("fast_task")
            else None,
        )

class FastTaskEnvironmentSpec(_common_models.FlyteIdlEntity):
    def __init__(
        self,
        image: str,
        replica_count: int,
    ):
        """
        :param image: TODO @hamersaw
        :param replica_count: TODO @hamersaw
        """
        self._image = image
        self._replica_count = replica_count

    @property
    def image(self) -> str:
        """
        TODO @hamersaw
        """
        return self._image

    @property
    def replica_count(self) -> int:
        """
        TODO @hamersaw
        """
        return self._replica_count

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_envs_pb2.FastTaskEnvironmentSpec
        """
        return _execution_envs_pb2.FastTaskEnvironmentSpec(
            image=self.image,
            replica_count=self.replica_count,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param TODO @hamersaw pb2_object:
        :return: TODO @hamersaw
        """
        return cls(
            image=pb2_object.image,
            replica_count=pb2_object.replica_count,
        )

class EnvironmentSpec(_common_models.FlyteIdlEntity):
    #class EnvironmentType(object):
    #    FAST_TASK = 0

    def __init__(
        self,
        type: int,
        fast_task: typing.Optional[FastTaskEnvironmentSpec] = None,
    ):
        """
        :param type: TODO @hamersaw
        :param fast_task: Optional, TODO @hamersaw
        """
        self._type = type
        self._fast_task = fast_task

    @property
    def type(self) -> int:
        """
        TODO @hamersaw
        """
        return self._type

    @property
    def fast_task(self) -> FastTaskEnvironmentSpec:
        """
        TODO @hamersaw
        """
        return self._fast_task

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_envs_pb2.ExecutionEnvironmentSpec
        """
        return _execution_envs_pb2.ExecutionEnvironmentSpec(
            type=self.type,
            fast_task=self.fast_task.to_flyte_idl()
            if self.fast_task is not None
            else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param TODO @hamersaw pb2_object:
        :return: TODO @hamersaw
        """
        return cls(
            type=pb2_object.type,
            fast_task=FastTaskEnvironmentSpec.from_flyte_idl(pb2_object.fast_task)
            if pb2_object.HasField("fast_task")
            else None,
        )

class ExecutionEnvironmentAssignment(_common_models.FlyteIdlEntity):
    def __init__(
        self,
        id: str,
        node_ids: typing.List[str],
        environment: Environment = None,
        environment_spec: EnvironmentSpec = None,
    ):
        """
        :param id: TODO @hamersaw
        :param node_ids: TODO @hamersaw
        :param environment: TODO @hamersaw
        """
        self._id = id
        self._node_ids = node_ids
        self._environment = environment
        self._environment_spec = environment_spec

    @property
    def id(self) -> int:
        """
        TODO @hamersaw
        """
        return self._id

    @property
    def node_ids(self) -> typing.List[str]:
        """
        TODO @hamersaw
        """
        return self._node_ids

    @property
    def environment(self) -> Environment:
        """
        TODO @hamersaw
        """
        return self._environment

    @property
    def environment_spec(self) -> EnvironmentSpec:
        """
        TODO @hamersaw
        """
        return self._environment_spec

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_envs_pb2.ExecutionEnvironmentAssignment
        """
        return _execution_envs_pb2.ExecutionEnvironmentAssignment(
            id=self.id,
            node_ids=self.node_ids,
            environment=self.environment.to_flyte_idl()
            if self.environment is not None
            else None,
            environment_spec=self.environment_spec.to_flyte_idl()
            if self.environment_spec is not None
            else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param TODO @hamersaw pb2_object:
        :return: TODO @hamersaw
        """
        return cls(
            id=pb2_object.id,
            node_ids=pb2_object.node_ids,
            environment=Environment.from_flyte_idl(pb2_object.environment)
            if pb2_object.HasField("environment")
            else None,
            environment_spec=EnvironmentSpec.from_flyte_idl(pb2_object.environment_spec)
            if pb2_object.HasField("environment_spec")
            else None,
        )
