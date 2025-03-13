import hashlib
import shelve
import typing
from dataclasses import dataclass
from enum import Enum

import grpc

from flytekit.core.store.graph_pb2 import CreateNodeRequest, Node, Status, ExecutionID, UpdateNodeStatusRequest, NodeID, \
    CreateExecutionRequest, TaskID
from flytekit.core.store.graph_pb2_grpc import GraphServiceStub
from google.protobuf import timestamp_pb2


def _calc_key(exec_id: str, name: str, input_kwargs: dict[str, typing.Any], node_group: str | None = None) -> str:
    """Make a deterministic name"""
    components = f"{exec_id}-{name}-{input_kwargs}" + (f"-{node_group}" if node_group else "")

    # has the components into something deterministic
    hex = hashlib.md5(components.encode()).hexdigest()
    exec_name = f"{hex}"
    return exec_name


class ItemStatus(Enum):
    PENDING = "Pending"
    RUNNING = "Running"
    SUCCESS = "Success"
    FAILED = "Failed"


@dataclass
class Data:
    input_kwargs: dict[str, typing.Any]
    result: typing.Any = None
    error: typing.Optional[BaseException] = None
    status: ItemStatus = ItemStatus.PENDING


class Store:

    def __init__(self, exec_id: str = "testing2"):
        self._store = shelve.open(f"{exec_id}.db")
        self._client = GraphServiceStub(grpc.insecure_channel('localhost:8080'))
        self._exec_id = exec_id
        self._create_execution()

    def _create_execution(self):
        print(f"Creating execution {self._exec_id}")
        self._client.CreateExecution(
            CreateExecutionRequest(
                execution_id=ExecutionID(id=self._exec_id),
                task_id=TaskID(id="root"),
            )
        )

    def add(self, name: str, value: Data, parent_key: str | None = None,
            node_group: str | None = None):
        print(f"Adding node {name} to store")
        key = _calc_key(self._exec_id, name, value.input_kwargs, node_group)
        n = Node(
            id=key,
            name=name,
            start_time=timestamp_pb2.Timestamp().GetCurrentTime(),
            status=Status.QUEUED,
            parent_node_id=parent_key if parent_key else None,
        )
        req = CreateNodeRequest(
            execution_id=ExecutionID(id=self._exec_id),
            node=n,
            parent_node_id=parent_key if parent_key else None,
            node_group=node_group if node_group else None,
        )
        res = self._client.CreateNode(req)
        self._store[key] = value

    def has(self, name: str, input_kwargs: dict[str, typing.Any], node_group: str | None = None) -> bool:
        key = _calc_key(self._exec_id, name, input_kwargs, node_group)
        return key in self._store

    def get(self, name: str, input_kwargs: dict[str, typing.Any], node_group: str | None = None) -> Data:
        key = _calc_key(self._exec_id, name, input_kwargs, node_group)
        return self._store[key]

    def update(self, name: str, value: Data, status: Status,
               node_group: str | None = None, error: str | None = None):
        print(f"Updating node {name} to store")
        key = _calc_key(self._exec_id, name, value.input_kwargs, node_group)
        self._client.UpdateNodeStatus(
            UpdateNodeStatusRequest(
                execution_id=ExecutionID(id=self._exec_id),
                node_id=NodeID(key),
                status=status,
                error_message=error if error else None,
            )
        )
        self._store[key] = value
