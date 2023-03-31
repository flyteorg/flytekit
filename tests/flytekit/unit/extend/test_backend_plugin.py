import typing
from unittest.mock import MagicMock

import grpc
from flyteidl.service.external_plugin_service_pb2 import (
    SUCCEEDED,
    TaskCreateResponse,
    TaskDeleteResponse,
    TaskGetResponse,
)

from flytekit.extend.backend.base_plugin import BackendPluginBase, BackendPluginRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class DummyPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="dummy")

    def initialize(self):
        pass

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> TaskCreateResponse:
        return TaskCreateResponse(job_id="dummy_id")

    def get(self, context: grpc.ServicerContext, job_id: str) -> TaskGetResponse:
        return TaskGetResponse(state=SUCCEEDED)

    def delete(self, context: grpc.ServicerContext, job_id) -> TaskDeleteResponse:
        return TaskDeleteResponse()


BackendPluginRegistry.register(DummyPlugin())


def test_base_plugin():
    p = BackendPluginBase(task_type="dummy")
    assert p.task_type == "dummy"
    ctx = MagicMock(spec=grpc.ServicerContext)
    p.create(ctx, "/tmp", None)
    p.get(ctx, "id")
    p.delete(ctx, "id")


def test_dummy_plugin():
    p = BackendPluginRegistry.get_plugin("dummy")
    ctx = MagicMock(spec=grpc.ServicerContext)
    assert p.create(ctx, "/tmp", None).job_id == "dummy_id"
    assert p.get(ctx, "id").state == SUCCEEDED
    assert p.delete(ctx, "id") == TaskDeleteResponse()
