import asyncio
import json
import typing
from dataclasses import asdict, dataclass
from datetime import timedelta
from unittest.mock import MagicMock, patch

import grpc
import pytest
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    RETRYABLE_FAILURE,
    RUNNING,
    SUCCEEDED,
    CreateTaskRequest,
    CreateTaskResponse,
    DeleteTaskRequest,
    DeleteTaskResponse,
    GetTaskRequest,
    GetTaskResponse,
    Resource,
)

import flytekit.models.interface as interface_models
from flytekit import PythonFunctionTask
from flytekit.extend.backend.agent_service import AsyncAgentService
from flytekit.extend.backend.base_agent import (
    AgentBase,
    AgentRegistry,
    AsyncAgentExecutorMixin,
    convert_to_flyte_state,
    get_agent_secret,
    is_terminal_state,
)
from flytekit.models import literals, task, types
from flytekit.models.core.identifier import Identifier, ResourceType
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

dummy_id = "dummy_id"
loop = asyncio.get_event_loop()


@dataclass
class Metadata:
    job_id: str


class DummyAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="dummy", asynchronous=False)

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        return CreateTaskResponse(resource_meta=json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8"))

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        return GetTaskResponse(resource=Resource(state=SUCCEEDED))

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()


class AsyncDummyAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="async_dummy", asynchronous=True)

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        return CreateTaskResponse(resource_meta=json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8"))

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        return GetTaskResponse(resource=Resource(state=SUCCEEDED))

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        return DeleteTaskResponse()


def get_task_template(task_type: str) -> TaskTemplate:
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="t1", version="version"
    )
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

    interfaces = interface_models.TypedInterface(
        {
            "a": interface_models.Variable(types.LiteralType(types.SimpleType.INTEGER), "description1"),
        },
        {},
    )

    return TaskTemplate(
        id=task_id,
        metadata=task_metadata,
        interface=interfaces,
        type=task_type,
        custom={},
    )


task_inputs = literals.LiteralMap(
    {
        "a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
    },
)

dummy_template = get_task_template("dummy")
async_dummy_template = get_task_template("async_dummy")


def test_dummy_agent():
    AgentRegistry.register(DummyAgent())
    ctx = MagicMock(spec=grpc.ServicerContext)
    agent = AgentRegistry.get_agent("dummy")
    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")
    assert agent.create(ctx, "/tmp", dummy_template, task_inputs).resource_meta == metadata_bytes
    assert agent.get(ctx, metadata_bytes).resource.state == SUCCEEDED
    assert agent.delete(ctx, metadata_bytes) == DeleteTaskResponse()

    class DummyTask(AsyncAgentExecutorMixin, PythonFunctionTask):
        def __init__(self, **kwargs):
            super().__init__(
                task_type="dummy",
                **kwargs,
            )

    t = DummyTask(task_config={}, task_function=lambda: None, container_image="dummy")
    t.execute()

    t._task_type = "non-exist-type"
    with pytest.raises(Exception, match="Cannot find agent for task type: non-exist-type."):
        t.execute()


@pytest.mark.asyncio
async def test_async_dummy_agent():
    AgentRegistry.register(AsyncDummyAgent())
    ctx = MagicMock(spec=grpc.ServicerContext)
    agent = AgentRegistry.get_agent("async_dummy")
    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")
    res = await agent.async_create(ctx, "/tmp", async_dummy_template, task_inputs)
    assert res.resource_meta == metadata_bytes
    res = await agent.async_get(ctx, metadata_bytes)
    assert res.resource.state == SUCCEEDED
    res = await agent.async_delete(ctx, metadata_bytes)
    assert res == DeleteTaskResponse()


@pytest.mark.asyncio
async def run_agent_server():
    service = AsyncAgentService()
    ctx = MagicMock(spec=grpc.ServicerContext)
    request = CreateTaskRequest(
        inputs=task_inputs.to_flyte_idl(), output_prefix="/tmp", template=dummy_template.to_flyte_idl()
    )
    async_request = CreateTaskRequest(
        inputs=task_inputs.to_flyte_idl(), output_prefix="/tmp", template=async_dummy_template.to_flyte_idl()
    )
    fake_agent = "fake"
    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")

    res = await service.CreateTask(request, ctx)
    assert res.resource_meta == metadata_bytes
    res = await service.GetTask(GetTaskRequest(task_type="dummy", resource_meta=metadata_bytes), ctx)
    assert res.resource.state == SUCCEEDED
    res = await service.DeleteTask(DeleteTaskRequest(task_type="dummy", resource_meta=metadata_bytes), ctx)
    assert isinstance(res, DeleteTaskResponse)

    res = await service.CreateTask(async_request, ctx)
    assert res.resource_meta == metadata_bytes
    res = await service.GetTask(GetTaskRequest(task_type="async_dummy", resource_meta=metadata_bytes), ctx)
    assert res.resource.state == SUCCEEDED
    res = await service.DeleteTask(DeleteTaskRequest(task_type="async_dummy", resource_meta=metadata_bytes), ctx)
    assert isinstance(res, DeleteTaskResponse)

    res = await service.GetTask(GetTaskRequest(task_type=fake_agent, resource_meta=metadata_bytes), ctx)
    assert res is None


def test_agent_server():
    loop.run_in_executor(None, run_agent_server)


def test_is_terminal_state():
    assert is_terminal_state(SUCCEEDED)
    assert is_terminal_state(PERMANENT_FAILURE)
    assert is_terminal_state(PERMANENT_FAILURE)
    assert not is_terminal_state(RUNNING)


def test_convert_to_flyte_state():
    assert convert_to_flyte_state("FAILED") == RETRYABLE_FAILURE
    assert convert_to_flyte_state("TIMEDOUT") == RETRYABLE_FAILURE
    assert convert_to_flyte_state("CANCELED") == RETRYABLE_FAILURE

    assert convert_to_flyte_state("DONE") == SUCCEEDED
    assert convert_to_flyte_state("SUCCEEDED") == SUCCEEDED
    assert convert_to_flyte_state("SUCCESS") == SUCCEEDED

    assert convert_to_flyte_state("RUNNING") == RUNNING

    invalid_state = "INVALID_STATE"
    with pytest.raises(Exception, match=f"Unrecognized state: {invalid_state.lower()}"):
        convert_to_flyte_state(invalid_state)


@patch("flytekit.current_context")
def test_get_agent_secret(mocked_context):
    mocked_context.return_value.secrets.get.return_value = "mocked token"
    assert get_agent_secret("mocked key") == "mocked token"
