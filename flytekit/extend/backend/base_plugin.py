import typing
from abc import abstractmethod

from flyteidl.core.tasks_pb2 import TaskTemplate
from flyteidl.service import plugin_system_pb2

from flytekit.models.literals import LiteralMap


class BackendPluginBase:
    def __init__(self, task_type: str):
        self._task_type = task_type

    @property
    def task_type(self) -> str:
        return self._task_type

    @abstractmethod
    def create(
        self, inputs: typing.Optional[LiteralMap], output_prefix: str, task_template: TaskTemplate
    ) -> plugin_system_pb2.TaskCreateResponse:
        pass

    @abstractmethod
    def get(self, job_id: str, prev_state: plugin_system_pb2.State) -> plugin_system_pb2.TaskGetResponse:
        pass

    @abstractmethod
    def delete(self, job_id: str) -> plugin_system_pb2.TaskDeleteResponse:
        pass


class BackendPluginRegistry(object):
    _REGISTRY: typing.Dict[str, BackendPluginBase] = {}

    @staticmethod
    def register(plugin: BackendPluginBase):
        BackendPluginRegistry._REGISTRY[plugin.task_type] = plugin

    @staticmethod
    def get_plugin(task_type: str) -> BackendPluginBase:
        return BackendPluginRegistry._REGISTRY[task_type]


def convert_to_flyte_state(state: str) -> plugin_system_pb2.State:
    if state.lower() in ["failed"]:
        return plugin_system_pb2.FAILED
    if state.lower() in ["done", "succeeded"]:
        return plugin_system_pb2.SUCCEEDED
    if state.lower() in ["running"]:
        return plugin_system_pb2.RUNNING
    raise ValueError("Unrecognize state")
