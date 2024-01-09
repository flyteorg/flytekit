import typing

from flyteidl.core import persistent_envs_pb2 as _persitent_envs_pb2

from flytekit.models import common as _common_models

class FastTaskEnvironment(_common_models.FlyteIdlEntity):
    def __init__(
        self,
        queue_id: str,
        namespace: str,
        pod_id: str,
    ):
        """
        :param type: TODO @hamersaw
        :param namespace: TODO @hamersaw
        :param podName: TODO @hamersaw
        """
        self._queue_id = queue_id
        self._namespace = namespace
        self._pod_id = pod_id

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
    def pod_id(self) -> str:
        """
        TODO @hamersaw
        """
        return self._pod_id

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.persitent_envs_pb2.Environment
        """
        return _persitent_envs_pb2.FastTaskEnvironment(
            queue_id=self.queue_id,
            namespace=self.namespace,
            pod_id=self.pod_id,
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
            pod_id=pb2_object.pod_id,
        )

class Environment(_common_models.FlyteIdlEntity):
    class EnvironmentType(object):
        FASTTASK = 0

    def __init__(
        self,
        type: int,
        fast_task_environment: typing.Optional[FastTaskEnvironment] = None,
    ):
        """
        :param type: TODO @hamersaw
        :param fast_task_environment: Optional, TODO @hamersaw
        """
        self._type = type
        self._fast_task_environment = fast_task_environment

    @property
    def type(self) -> int:
        """
        TODO @hamersaw
        """
        return self._type

    @property
    def fast_task_environment(self) -> FastTaskEnvironment:
        """
        TODO @hamersaw
        """
        return self._fast_task_environment

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.persitent_envs_pb2.Environment
        """
        return _persitent_envs_pb2.Environment(
            type=self.type,
            fasttask_environment=self.fast_task_environment.to_flyte_idl()
            if self.fast_task_environment is not None
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
            fast_task_environment=FastTaskEnvironment.from_flyte_idl(pb2_object.fasttask_environment)
            if pb2_object.HasField("fasttask_environment")
            else None,
        )

class EnvironmentAssignment(_common_models.FlyteIdlEntity):
    def __init__(
        self,
        id: str,
        node_ids: typing.List[str],
        environment: Environment,
    ):
        """
        :param id: TODO @hamersaw
        :param node_ids: TODO @hamersaw
        :param environment: TODO @hamersaw
        """
        self._id = id
        self._node_ids = node_ids
        self._environment = environment

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

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.persitent_envs_pb2.Environment
        """
        return _persitent_envs_pb2.EnvironmentAssignment(
            id=self.id,
            node_ids=self.node_ids,
            environment=self.environment.to_flyte_idl(),
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
            environment=Environment.from_flyte_idl(pb2_object.environment),
        )
