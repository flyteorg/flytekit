from dataclasses import dataclass
import typing
from abc import abstractmethod
from typing import Optional

from flyteidl.core.tasks_pb2 import TaskTemplate

from flytekit.models.interface import VariableMap

PENDING = "pending"
SUCCEEDED = "succeeded"
RUNNING = "running"


@dataclass
class CreateRequest:
    inputs: VariableMap
    task_template: TaskTemplate


@dataclass
class CreateResponse:
    job_id: str
    message: Optional[str] = None


@dataclass
class PollRequest:
    job_id: str
    output_prefix: str
    prev_state: str


@dataclass
class PollResponse:
    state: str
    message: Optional[str] = None


class BackendPluginBase:
    def __init__(self, task_type: str):
        self._task_type = task_type

    @property
    def task_type(self) -> str:
        return self._task_type

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def create(self, create_request: CreateRequest) -> CreateResponse:
        pass

    @abstractmethod
    def poll(self, poll_request: PollRequest) -> PollResponse:
        pass

    @abstractmethod
    def terminate(self, job_id: str):
        pass


class BackendPluginRegistry(object):
    _REGISTRY: typing.Dict[str, BackendPluginBase] = {}

    @staticmethod
    def register(plugin: BackendPluginBase):
        BackendPluginRegistry._REGISTRY[plugin.task_type] = plugin

    @staticmethod
    def get_plugin(task_type: str) -> BackendPluginBase:
        return BackendPluginRegistry._REGISTRY[task_type]


def convert_to_flyte_state(state: str) -> str:
    if state.lower() in [PENDING]:
        return PENDING
    if state.lower() in ["done", SUCCEEDED]:
        return SUCCEEDED
    if state.lower() in [RUNNING]:
        return RUNNING
    raise ValueError("Unrecognize state")
