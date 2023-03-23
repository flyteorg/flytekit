import typing

from flyteidl.service import plugin_system_pb2

from flytekit.extend.backend.base_plugin import BackendPluginBase, BackendPluginRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class DummyPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="bigquery")

    def initialize(self):
        pass

    def create(
        self, inputs: typing.Optional[LiteralMap], output_prefix: str, task_template: TaskTemplate
    ) -> plugin_system_pb2.TaskCreateResponse:
        return plugin_system_pb2.TaskCreateResponse(job_id="dummy_id")

    def get(self, job_id: str) -> plugin_system_pb2.TaskGetResponse:
        return plugin_system_pb2.TaskGetResponse(state=plugin_system_pb2.RUNNING)

    def delete(self, job_id) -> plugin_system_pb2.TaskDeleteResponse:
        return plugin_system_pb2.TaskDeleteResponse()


BackendPluginRegistry.register(DummyPlugin())
