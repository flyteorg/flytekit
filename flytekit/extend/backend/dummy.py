import typing

import grpc
from flyteidl.service import external_plugin_service_pb2

from flytekit.extend.backend.base_plugin import BackendPluginBase, BackendPluginRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class DummyPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="bigquery_query_job_task")

    def initialize(self):
        pass

    def create(
        self, context: grpc.ServicerContext, output_prefix: str, task_template: TaskTemplate, inputs: typing.Optional[LiteralMap] = None
    ) -> external_plugin_service_pb2.TaskCreateResponse:
        print("create called")
        return external_plugin_service_pb2.TaskCreateResponse(job_id="dummy_id")

    def get(self, context: grpc.ServicerContext, job_id: str) -> external_plugin_service_pb2.TaskGetResponse:
        print("get called for id: " + job_id)
        return external_plugin_service_pb2.TaskGetResponse(state=external_plugin_service_pb2.SUCCEEDED)

    def delete(self, context: grpc.ServicerContext, job_id) -> external_plugin_service_pb2.TaskDeleteResponse:
        print("delete called" + job_id)
        return external_plugin_service_pb2.TaskDeleteResponse()


BackendPluginRegistry.register(DummyPlugin())
