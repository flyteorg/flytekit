from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = []
    UNDEFINED: _ClassVar[Status]
    QUEUED: _ClassVar[Status]
    RUNNING: _ClassVar[Status]
    SUCCEEDING: _ClassVar[Status]
    SUCCEEDED: _ClassVar[Status]
    FAILING: _ClassVar[Status]
    FAILED: _ClassVar[Status]
    ABORTED: _ClassVar[Status]
    TIMED_OUT: _ClassVar[Status]
    RECOVERED: _ClassVar[Status]
    ABORTING: _ClassVar[Status]
UNDEFINED: Status
QUEUED: Status
RUNNING: Status
SUCCEEDING: Status
SUCCEEDED: Status
FAILING: Status
FAILED: Status
ABORTED: Status
TIMED_OUT: Status
RECOVERED: Status
ABORTING: Status

class TaskID(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ExecutionID(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class NodeID(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class Task(_message.Message):
    __slots__ = ["id", "name", "description", "command", "inputs", "outputs"]
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    COMMAND_FIELD_NUMBER: _ClassVar[int]
    INPUTS_FIELD_NUMBER: _ClassVar[int]
    OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    id: TaskID
    name: str
    description: str
    command: str
    inputs: _containers.RepeatedScalarFieldContainer[str]
    outputs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[_Union[TaskID, _Mapping]] = ..., name: _Optional[str] = ..., description: _Optional[str] = ..., command: _Optional[str] = ..., inputs: _Optional[_Iterable[str]] = ..., outputs: _Optional[_Iterable[str]] = ...) -> None: ...

class ExecutionInfo(_message.Message):
    __slots__ = ["execution_id", "status", "start_time", "end_time"]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    execution_id: ExecutionID
    status: Status
    start_time: _timestamp_pb2.Timestamp
    end_time: _timestamp_pb2.Timestamp
    def __init__(self, execution_id: _Optional[_Union[ExecutionID, _Mapping]] = ..., status: _Optional[_Union[Status, str]] = ..., start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., end_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class Node(_message.Message):
    __slots__ = ["id", "name", "status", "start_time", "end_time", "parent_node_id", "properties"]
    class PropertiesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    PARENT_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    id: str
    name: str
    status: Status
    start_time: _timestamp_pb2.Timestamp
    end_time: _timestamp_pb2.Timestamp
    parent_node_id: str
    properties: _containers.ScalarMap[str, str]
    def __init__(self, id: _Optional[str] = ..., name: _Optional[str] = ..., status: _Optional[_Union[Status, str]] = ..., start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., end_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., parent_node_id: _Optional[str] = ..., properties: _Optional[_Mapping[str, str]] = ...) -> None: ...

class Edge(_message.Message):
    __slots__ = ["source", "target", "properties"]
    class PropertiesEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    PROPERTIES_FIELD_NUMBER: _ClassVar[int]
    source: str
    target: str
    properties: _containers.ScalarMap[str, str]
    def __init__(self, source: _Optional[str] = ..., target: _Optional[str] = ..., properties: _Optional[_Mapping[str, str]] = ...) -> None: ...

class NodeInfo(_message.Message):
    __slots__ = ["nodes", "edges"]
    NODES_FIELD_NUMBER: _ClassVar[int]
    EDGES_FIELD_NUMBER: _ClassVar[int]
    nodes: _containers.RepeatedCompositeFieldContainer[Node]
    edges: _containers.RepeatedCompositeFieldContainer[Edge]
    def __init__(self, nodes: _Optional[_Iterable[_Union[Node, _Mapping]]] = ..., edges: _Optional[_Iterable[_Union[Edge, _Mapping]]] = ...) -> None: ...

class CreateExecutionRequest(_message.Message):
    __slots__ = ["task_id", "execution_id"]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    task_id: TaskID
    execution_id: ExecutionID
    def __init__(self, task_id: _Optional[_Union[TaskID, _Mapping]] = ..., execution_id: _Optional[_Union[ExecutionID, _Mapping]] = ...) -> None: ...

class CreateNodeRequest(_message.Message):
    __slots__ = ["execution_id", "node", "parent_node_id", "node_group"]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_FIELD_NUMBER: _ClassVar[int]
    PARENT_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_GROUP_FIELD_NUMBER: _ClassVar[int]
    execution_id: ExecutionID
    node: Node
    parent_node_id: str
    node_group: str
    def __init__(self, execution_id: _Optional[_Union[ExecutionID, _Mapping]] = ..., node: _Optional[_Union[Node, _Mapping]] = ..., parent_node_id: _Optional[str] = ..., node_group: _Optional[str] = ...) -> None: ...

class Worker(_message.Message):
    __slots__ = ["id"]
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...

class ErrorMessage(_message.Message):
    __slots__ = ["code", "message", "error_kind", "timestamp", "worker"]
    class ErrorKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
        UNKNOWN: _ClassVar[ErrorMessage.ErrorKind]
        SYSTEM: _ClassVar[ErrorMessage.ErrorKind]
        USER: _ClassVar[ErrorMessage.ErrorKind]
    UNKNOWN: ErrorMessage.ErrorKind
    SYSTEM: ErrorMessage.ErrorKind
    USER: ErrorMessage.ErrorKind
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    ERROR_KIND_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    WORKER_FIELD_NUMBER: _ClassVar[int]
    code: str
    message: str
    error_kind: ErrorMessage.ErrorKind
    timestamp: _timestamp_pb2.Timestamp
    worker: Worker
    def __init__(self, code: _Optional[str] = ..., message: _Optional[str] = ..., error_kind: _Optional[_Union[ErrorMessage.ErrorKind, str]] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., worker: _Optional[_Union[Worker, _Mapping]] = ...) -> None: ...

class UpdateNodeStatusRequest(_message.Message):
    __slots__ = ["execution_id", "node_id", "status", "parent_node_id", "error_message", "reason"]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    PARENT_NODE_ID_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    execution_id: ExecutionID
    node_id: NodeID
    status: Status
    parent_node_id: NodeID
    error_message: ErrorMessage
    reason: str
    def __init__(self, execution_id: _Optional[_Union[ExecutionID, _Mapping]] = ..., node_id: _Optional[_Union[NodeID, _Mapping]] = ..., status: _Optional[_Union[Status, str]] = ..., parent_node_id: _Optional[_Union[NodeID, _Mapping]] = ..., error_message: _Optional[_Union[ErrorMessage, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class WatchAllExecutionsRequest(_message.Message):
    __slots__ = ["task_id", "after"]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    AFTER_FIELD_NUMBER: _ClassVar[int]
    task_id: TaskID
    after: _timestamp_pb2.Timestamp
    def __init__(self, task_id: _Optional[_Union[TaskID, _Mapping]] = ..., after: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class WatchNodesForExecutionRequest(_message.Message):
    __slots__ = ["execution_id"]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    execution_id: ExecutionID
    def __init__(self, execution_id: _Optional[_Union[ExecutionID, _Mapping]] = ...) -> None: ...

class UpdateExecutionStatusRequest(_message.Message):
    __slots__ = ["execution_id", "status", "error_message", "reason"]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    execution_id: ExecutionID
    status: Status
    error_message: ErrorMessage
    reason: str
    def __init__(self, execution_id: _Optional[_Union[ExecutionID, _Mapping]] = ..., status: _Optional[_Union[Status, str]] = ..., error_message: _Optional[_Union[ErrorMessage, _Mapping]] = ..., reason: _Optional[str] = ...) -> None: ...

class GetNodeDetailsRequest(_message.Message):
    __slots__ = ["execution_id", "node_id"]
    EXECUTION_ID_FIELD_NUMBER: _ClassVar[int]
    NODE_ID_FIELD_NUMBER: _ClassVar[int]
    execution_id: ExecutionID
    node_id: NodeID
    def __init__(self, execution_id: _Optional[_Union[ExecutionID, _Mapping]] = ..., node_id: _Optional[_Union[NodeID, _Mapping]] = ...) -> None: ...

class NodeDetails(_message.Message):
    __slots__ = ["node", "status", "error_message", "reason", "start_time", "end_time", "task_id", "other_details"]
    class OtherDetailsEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    NODE_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    TASK_ID_FIELD_NUMBER: _ClassVar[int]
    OTHER_DETAILS_FIELD_NUMBER: _ClassVar[int]
    node: Node
    status: Status
    error_message: ErrorMessage
    reason: str
    start_time: _timestamp_pb2.Timestamp
    end_time: _timestamp_pb2.Timestamp
    task_id: TaskID
    other_details: _containers.ScalarMap[str, str]
    def __init__(self, node: _Optional[_Union[Node, _Mapping]] = ..., status: _Optional[_Union[Status, str]] = ..., error_message: _Optional[_Union[ErrorMessage, _Mapping]] = ..., reason: _Optional[str] = ..., start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., end_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., task_id: _Optional[_Union[TaskID, _Mapping]] = ..., other_details: _Optional[_Mapping[str, str]] = ...) -> None: ...
