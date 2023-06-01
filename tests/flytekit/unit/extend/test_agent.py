import json
import typing
from dataclasses import asdict, dataclass
from datetime import timedelta
from unittest.mock import MagicMock

import grpc
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    SUCCEEDED,
    CreateTaskRequest,
    CreateTaskResponse,
    DeleteTaskRequest,
    DeleteTaskResponse,
    GetTaskRequest,
    GetTaskResponse,
    resource,
)

import flytekit.models.interface as interface_models
from flytekit.extend.backend.agent_service import AgentService
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models import literals, task, types
from flytekit.models.core.identifier import Identifier, ResourceType
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

dummy_id = "dummy_id"


@dataclass
class Metadata:
    job_id: str


class DummyAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="dummy")

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        return CreateTaskResponse(resource_meta=json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8"))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        return GetTaskResponse(resource=resource(state=SUCCEEDED))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()


AgentRegistry.register(DummyAgent())

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


def test_dummy_agent():
    ctx = MagicMock(spec=grpc.ServicerContext)
    agent = AgentRegistry.get_agent(ctx, "dummy")
    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")
    assert agent.create(ctx, "/tmp", dummy_template, task_inputs).resource_meta == metadata_bytes
    assert agent.get(ctx, metadata_bytes).resource.state == SUCCEEDED
    assert agent.delete(ctx, metadata_bytes) == DeleteTaskResponse()


def test_agent_server():
    service = AgentService()
    ctx = MagicMock(spec=grpc.ServicerContext)
    request = CreateTaskRequest(
        inputs=task_inputs.to_flyte_idl(), output_prefix="/tmp", template=dummy_template.to_flyte_idl()
    )

    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")
    assert service.CreateTask(request, ctx).resource_meta == metadata_bytes
    assert (
        service.GetTask(GetTaskRequest(task_type="dummy", resource_meta=metadata_bytes), ctx).resource.state
        == SUCCEEDED
    )
    assert (
        service.DeleteTask(DeleteTaskRequest(task_type="dummy", resource_meta=metadata_bytes), ctx)
        == DeleteTaskResponse()
    )

    res = service.GetTask(GetTaskRequest(task_type="fake", resource_meta=metadata_bytes), ctx)
    assert res.resource.state == PERMANENT_FAILURE
