import typing
from abc import ABC, abstractmethod

import grpc
from flyteidl.core.tasks_pb2 import TaskTemplate
from flyteidl.service.external_plugin_service_pb2 import (
    RETRYABLE_FAILURE,
    RUNNING,
    SUCCEEDED,
    State,
    TaskCreateResponse,
    TaskDeleteResponse,
    TaskGetResponse,
)

from flytekit.models.literals import LiteralMap


class BackendPluginBase(ABC):
    def __init__(self, task_type: str):
        self._task_type = task_type

    @property
    def task_type(self) -> str:
        return self._task_type

    @abstractmethod
    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> TaskCreateResponse:
        pass

    @abstractmethod
    def get(self, context: grpc.ServicerContext, job_id: str) -> TaskGetResponse:
        pass

    @abstractmethod
    def delete(self, context: grpc.ServicerContext, job_id: str) -> TaskDeleteResponse:
        pass


class BackendPluginRegistry(object):
    _REGISTRY: typing.Dict[str, BackendPluginBase] = {}

    @staticmethod
    def register(plugin: BackendPluginBase):
        BackendPluginRegistry._REGISTRY[plugin.task_type] = plugin

    @staticmethod
    def get_plugin(task_type: str) -> BackendPluginBase:
        return BackendPluginRegistry._REGISTRY[task_type]


def convert_to_flyte_state(state: str) -> State:
    state = state.lower()
    if state in ["failed"]:
        return RETRYABLE_FAILURE
    elif state in ["done", "succeeded"]:
        return SUCCEEDED
    elif state in ["running"]:
        return RUNNING
    raise ValueError(f"Unrecognized state: {state}")
