import json as _json
import typing

from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flyteidl.core import execution_envs_pb2 as _execution_envs_pb2

from flytekit.models import common as _common_models

class Environment(_common_models.FlyteIdlEntity):
    #class EnvironmentType(object):
    #    FAST_TASK = 0

    def __init__(
        self,
        type: int,
        #fast_task: typing.Optional[FastTaskEnvironment] = None,
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

    #@property
    #def fast_task(self) -> FastTaskEnvironment:
    #    """
    #    TODO @hamersaw
    #    """
    #    return self._fast_task

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_envs_pb2.ExecutionEnvironment
        """
        return _execution_envs_pb2.ExecutionEnvironment(
            type=self.type,
            #fast_task=self.fast_task.to_flyte_idl()
            #if self.fast_task is not None
            #else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param TODO @hamersaw pb2_object:
        :return: TODO @hamersaw
        """
        return cls(
            type=pb2_object.type,
            #fast_task=FastTaskEnvironment.from_flyte_idl(pb2_object.fast_task)
            #if pb2_object.HasField("fast_task")
            #else None,
        )

class EnvironmentSpec(_common_models.FlyteIdlEntity):
    #class EnvironmentType(object):
    #    FAST_TASK = 0

    def __init__(
        self,
        type: str,
        spec,
        #fast_task: typing.Optional[FastTaskEnvironmentSpec] = None,
    ):
        """
        :param type: TODO @hamersaw
        :param fast_task: Optional, TODO @hamersaw
        """
        self._type = type
        #self._fast_task = fast_task
        self._spec = spec

    @property
    def type(self) -> str:
        """
        TODO @hamersaw
        """
        return self._type

    #@property
    #def fast_task(self) -> FastTaskEnvironmentSpec:
    #    """
    #    TODO @hamersaw
    #    """
    #    return self._fast_task
    @property
    def spec(self):
        """
        TODO @hamersaw
        """
        return self._spec

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_envs_pb2.ExecutionEnvironmentSpec
        """
        print(_json.dumps(self.spec))
        return _execution_envs_pb2.ExecutionEnvironmentSpec(
            type=self.type,
            spec=_json_format.Parse(_json.dumps(self.spec), _struct.Struct()) if self.spec else None,
            #fast_task=self.fast_task.to_flyte_idl()
            #if self.fast_task is not None
            #else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param TODO @hamersaw pb2_object:
        :return: TODO @hamersaw
        """
        return cls(
            type=pb2_object.type,
            spec=_json_format.MessageToDict(pb2_object.spec) if pb2_object else None,
            #fast_task=FastTaskEnvironmentSpec.from_flyte_idl(pb2_object.fast_task)
            #if pb2_object.HasField("fast_task")
            #else None,
        )

class ExecutionEnvironmentAssignment(_common_models.FlyteIdlEntity):
    def __init__(
        self,
        id: str,
        type: str,
        node_ids: typing.List[str],
        environment: None,
        environment_spec: None,
    ):
        """
        :param id: TODO @hamersaw
        :param node_ids: TODO @hamersaw
        :param environment: TODO @hamersaw
        """
        self._id = id
        self._node_ids = node_ids
        self._type = type
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
    def type(self) -> str:
        """
        TODO @hamersaw
        """
        return self._type

    @property
    def environment(self):
        """
        TODO @hamersaw
        """
        return self._environment

    @property
    def environment_spec(self):
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
            type=self.type,
            environment=_json_format.Parse(_json.dumps(self.environment), _struct.Struct()) if self.environment else None,
            environment_spec=_json_format.Parse(_json.dumps(self.environment_spec), _struct.Struct()) if self.environment_spec else None,
            #environment=self.environment.to_flyte_idl()
            #if self.environment is not None
            #else None,
            #environment_spec=self.environment_spec.to_flyte_idl()
            #if self.environment_spec is not None
            #else None,
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
            type=pb2_object.type,
            environment=_json_format.MessageToDict(pb2_object.environment) if pb2_object.environment else None,
            environment_spec=_json_format.MessageToDict(pb2_object.environment_spec) if pb2_object.environment_spec else None,
            #environment=Environment.from_flyte_idl(pb2_object.environment)
            #if pb2_object.HasField("environment")
            #else None,
            #environment_spec=EnvironmentSpec.from_flyte_idl(pb2_object.environment_spec)
            #if pb2_object.HasField("environment_spec")
            #else None,
        )
