import json as _json
import typing

from google.protobuf import json_format as _json_format
from google.protobuf import struct_pb2 as _struct

from flyteidl.core import execution_envs_pb2 as _execution_envs_pb2

from flytekit.models import common as _common_models

class ExecutionEnvironment(_common_models.FlyteIdlEntity):
    def __init__(
        self,
        id: str,
        type: str,
        extant,
        spec,
    ):
        """
        :param type: TODO @hamersaw
        :param fast_task: Optional, TODO @hamersaw
        """
        self._id = id
        self._type = type
        self._extant = extant
        self._spec = spec

    @property
    def id(self) -> str:
        """
        TODO @hamersaw
        """
        return self._id

    @property
    def type(self) -> str:
        """
        TODO @hamersaw
        """
        return self._type

    @property
    def extant(self):
        """
        TODO @hamersaw
        """
        return self._extant

    @property
    def spec(self):
        """
        TODO @hamersaw
        """
        return self._spec

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_envs_pb2.ExecutionEnvironment
        """
        return _execution_envs_pb2.ExecutionEnvironment(
            id=self.id,
            type=self.type,
            extant=_json_format.Parse(_json.dumps(self.extant), _struct.Struct()) if self.extant else None,
            spec=_json_format.Parse(_json.dumps(self.spec), _struct.Struct()) if self.spec else None,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        """
        :param TODO @hamersaw pb2_object:
        :return: TODO @hamersaw
        """
        return cls(
            id=pb2_object.id,
            type=pb2_object.type,
            extant=_json_format.MessageToDict(pb2_object.extant) if pb2_object.extant else None,
            spec=_json_format.MessageToDict(pb2_object.spec) if pb2_object.spec else None,
        )

class ExecutionEnvAssignment(_common_models.FlyteIdlEntity):
    def __init__(
        self,
        node_ids: typing.List[str],
        task_type: str,
        execution_env: None,
    ):
        """
        :param id: TODO @hamersaw
        :param node_ids: TODO @hamersaw
        :param environment: TODO @hamersaw
        """
        self._node_ids = node_ids
        self._task_type = task_type
        self._execution_env = execution_env

    @property
    def node_ids(self) -> typing.List[str]:
        """
        TODO @hamersaw
        """
        return self._node_ids

    @property
    def task_type(self) -> str:
        """
        TODO @hamersaw
        """
        return self._task_type

    @property
    def execution_env(self):
        """
        TODO @hamersaw
        """
        return self._execution_env

    def to_flyte_idl(self):
        """
        :rtype: flyteidl.core.execution_envs_pb2.ExecutionEnvironmentAssignment
        """
        return _execution_envs_pb2.ExecutionEnvAssignment(
            node_ids=self.node_ids,
            task_type=self.task_type,
            #environment=_json_format.Parse(_json.dumps(self.environment), _struct.Struct()) if self.environment else None,
            #environment_spec=_json_format.Parse(_json.dumps(self.environment_spec), _struct.Struct()) if self.environment_spec else None,
            execution_env=self.execution_env.to_flyte_idl()
            if self.execution_env is not None
            else None,
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
            node_ids=pb2_object.node_ids,
            task_type=pb2_object.task_type,
            #environment=_json_format.MessageToDict(pb2_object.environment) if pb2_object.environment else None,
            #environment_spec=_json_format.MessageToDict(pb2_object.environment_spec) if pb2_object.environment_spec else None,
            execution_env=ExecutionEnvironment.from_flyte_idl(pb2_object.execution_env)
            if pb2_object.HasField("execution_env")
            else None,
            #environment_spec=EnvironmentSpec.from_flyte_idl(pb2_object.environment_spec)
            #if pb2_object.HasField("environment_spec")
            #else None,
        )
