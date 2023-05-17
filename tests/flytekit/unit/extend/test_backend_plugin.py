import typing
from datetime import timedelta
from unittest.mock import MagicMock

import grpc
from flyteidl.service.external_plugin_service_pb2 import (
    PERMANENT_FAILURE,
    SUCCEEDED,
    TaskCreateRequest,
    TaskCreateResponse,
    TaskDeleteRequest,
    TaskDeleteResponse,
    TaskGetRequest,
    TaskGetResponse,
)

import flytekit.models.interface as interface_models
from flytekit.extend.backend.base_plugin import BackendPluginBase, BackendPluginRegistry
from flytekit.extend.backend.external_plugin_service import BackendPluginServer
from flytekit.models import literals, task, types
from flytekit.models.core.identifier import Identifier, ResourceType
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

dummy_id = "dummy_id"


class DummyPlugin(BackendPluginBase):
    def __init__(self):
        super().__init__(task_type="dummy")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> TaskCreateResponse:
        return TaskCreateResponse(job_id=dummy_id)

    def get(self, context: grpc.ServicerContext, job_id: str) -> TaskGetResponse:
        return TaskGetResponse(state=SUCCEEDED)

    def delete(self, context: grpc.ServicerContext, job_id) -> TaskDeleteResponse:
        return TaskDeleteResponse()


BackendPluginRegistry.register(DummyPlugin())

task_id = Identifier(resource_type=ResourceType.TASK, project="project", domain="domain", name="t1", version="version")
task_metadata = task.TaskMetadata(
    True,
    task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
    timedelta(days=1),
    literals.RetryStrategy(3),
    True,
    "0.1.1b0",
    "This is deprecated!",
    True,
    "A",
)

int_type = types.LiteralType(types.SimpleType.INTEGER)
interfaces = interface_models.TypedInterface(
    {
        "a": interface_models.Variable(int_type, "description1"),
    },
    {},
)
task_inputs = literals.LiteralMap(
    {
        "a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
    },
)

dummy_template = TaskTemplate(
    id=task_id,
    metadata=task_metadata,
    interface=interfaces,
    type="dummy",
    custom={},
)


def test_dummy_plugin():
    ctx = MagicMock(spec=grpc.ServicerContext)
    p = BackendPluginRegistry.get_plugin(ctx, "dummy")
    assert p.create(ctx, "/tmp", dummy_template, task_inputs).job_id == dummy_id
    assert p.get(ctx, dummy_id).state == SUCCEEDED
    assert p.delete(ctx, dummy_id) == TaskDeleteResponse()


def test_backend_plugin_server():
    server = BackendPluginServer()
    ctx = MagicMock(spec=grpc.ServicerContext)
    request = TaskCreateRequest(
        inputs=task_inputs.to_flyte_idl(), output_prefix="/tmp", template=dummy_template.to_flyte_idl()
    )

    assert server.CreateTask(request, ctx).job_id == dummy_id
    assert server.GetTask(TaskGetRequest(task_type="dummy", job_id=dummy_id), ctx).state == SUCCEEDED
    assert server.DeleteTask(TaskDeleteRequest(task_type="dummy", job_id=dummy_id), ctx) == TaskDeleteResponse()

    res = server.GetTask(TaskGetRequest(task_type="fake", job_id=dummy_id), ctx)
    assert res.state == PERMANENT_FAILURE
