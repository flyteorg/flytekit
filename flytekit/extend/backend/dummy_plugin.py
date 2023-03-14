from random import randint

from flyteidl.service import plugin_system_pb2

from flytekit.extend.backend.base_plugin import BackendPluginBase, BackendPluginRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


# This plugin is used for performance benchmarking
# will remove this file before pr is merged
class DummyPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="dummy")

    def initialize(self):
        pass

    def create(
        self, inputs: LiteralMap, output_prefix: str, task_template: TaskTemplate
    ) -> plugin_system_pb2.TaskCreateResponse:
        print("creating")
        return plugin_system_pb2.TaskCreateResponse(job_id="fake_id")

    def get(self, job_id: str, prev_state: plugin_system_pb2.State) -> plugin_system_pb2.TaskGetResponse:
        print("polling")
        if prev_state == plugin_system_pb2.SUCCEEDED:
            return plugin_system_pb2.TaskGetResponse(state=plugin_system_pb2.SUCCEEDED)

        x = randint(1, 100)
        if x > 50:
            state = plugin_system_pb2.SUCCEEDED
        else:
            state = plugin_system_pb2.RUNNING

        return plugin_system_pb2.TaskGetResponse(state=state)

    def delete(self, job_id) -> plugin_system_pb2.TaskDeleteResponse:
        print("deleting")
        return plugin_system_pb2.TaskDeleteResponse()


BackendPluginRegistry.register(DummyPlugin())
