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
    DoTaskRequest,
    DoTaskResponse,
    GetTaskRequest,
    GetTaskResponse,
    Resource,
)

import flytekit.models.interface as interface_models
from flytekit import PythonFunctionTask
from flytekit.extend.backend.agent_service import AsyncAgentService, SyncAgentService
from flytekit.extend.backend.base_agent import (
    AgentBase,
    AgentRegistry,
    AsyncAgentExecutorMixin,
    SyncAgentExecutorMixin,
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


class SyncDummyAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="sync_dummy", asynchronous=True)

    async def async_do(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> DoTaskResponse:
        return DoTaskResponse(resource=Resource(state=SUCCEEDED))

    def do(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> DoTaskResponse:
        return DoTaskResponse(resource=Resource(state=SUCCEEDED))


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


def get_task_template(task_type: str, is_sync_plugin: bool = False) -> TaskTemplate:
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="t1", version="version"
    )
    task_metadata = task.TaskMetadata(
        True,
        task.RuntimeMetadata(task.RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python", is_sync_plugin),
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


async_dummy_template = get_task_template("async_dummy")
sync_dummy_template = get_task_template("sync_dummy", True)


def test_dummy_agent():
    ctx = MagicMock(spec=grpc.ServicerContext)
    async_agent = AgentRegistry.get_agent("async_dummy")
    sync_agent = AgentRegistry.get_agent("sync_dummy")
    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")
    assert async_agent.create(ctx, "/tmp", async_dummy_template, task_inputs).resource_meta == metadata_bytes
    assert async_agent.get(ctx, metadata_bytes).resource.state == SUCCEEDED
    assert async_agent.delete(ctx, metadata_bytes) == DeleteTaskResponse()
    assert sync_agent.do(ctx, sync_dummy_template, task_inputs) == DoTaskResponse(resource=Resource(state=SUCCEEDED))

    class AsyncDummyTask(AsyncAgentExecutorMixin, PythonFunctionTask):
        def __init__(self, **kwargs):
            super().__init__(
                task_type="async_dummy",
                is_sync_plugin=False,
                **kwargs,
            )

    t = AsyncDummyTask(task_config={}, task_function=lambda: None, container_image="dummy")
    t.execute()

    class SyncDummyTask(SyncAgentExecutorMixin, PythonFunctionTask):
        def __init__(self, **kwargs):
            super().__init__(
                task_type="sync_dummy",
                is_sync_plugin=True,
                **kwargs,
            )

    t = SyncDummyTask(task_config={}, task_function=lambda: None, container_image="sync_dummy")
    t.execute()

    t._task_type = "non-exist-type"
    with pytest.raises(Exception, match="Cannot find agent for task type: non-exist-type."):
        t.execute()


@pytest.mark.asyncio
async def test_async_dummy_agent():
    ctx = MagicMock(spec=grpc.ServicerContext)
    async_agent = AgentRegistry.get_agent("async_dummy")
    sync_agent = AgentRegistry.get_agent("sync_dummy")
    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")
    res = await async_agent.async_create(ctx, "/tmp", async_dummy_template, task_inputs)
    assert res.resource_meta == metadata_bytes
    res = await async_agent.async_get(ctx, metadata_bytes)
    assert res.resource.state == SUCCEEDED
    res = await async_agent.async_delete(ctx, metadata_bytes)
    assert res == DeleteTaskResponse()
    res = await sync_agent.async_do(ctx, "/tmp", sync_dummy_template, task_inputs)
    assert res == DoTaskResponse(resource=Resource(state=SUCCEEDED))


@pytest.mark.asyncio
async def run_agent_server():
    async_agent_service = AsyncAgentService()
    sync_agent_service = SyncAgentService()

    ctx = MagicMock(spec=grpc.ServicerContext)
    create_request = CreateTaskRequest(
        inputs=task_inputs.to_flyte_idl(), output_prefix="/tmp", template=async_dummy_template.to_flyte_idl()
    )
    async_create_request = CreateTaskRequest(
        inputs=task_inputs.to_flyte_idl(), output_prefix="/tmp", template=async_dummy_template.to_flyte_idl()
    )
    do_request = DoTaskRequest(
        inputs=task_inputs.to_flyte_idl(), output_prefix="/tmp", template=sync_dummy_template.to_flyte_idl()
    )
    async_do_request = DoTaskRequest(
        inputs=task_inputs.to_flyte_idl(), output_prefix="/tmp", template=sync_dummy_template.to_flyte_idl()
    )
    fake_agent = "fake"
    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")

    res = await async_agent_service.CreateTask(create_request, ctx)
    assert res.resource_meta == metadata_bytes
    res = await async_agent_service.GetTask(GetTaskRequest(task_type="async_dummy", resource_meta=metadata_bytes), ctx)
    assert res.resource.state == SUCCEEDED
    res = await async_agent_service.DeleteTask(
        DeleteTaskRequest(task_type="async_dummy", resource_meta=metadata_bytes), ctx
    )
    assert isinstance(res, DeleteTaskResponse)
    res = await sync_agent_service.DoTask(do_request, ctx)
    assert res.resource.state == SUCCEEDED

    res = await async_agent_service.CreateTask(async_create_request, ctx)
    assert res.resource_meta == metadata_bytes
    res = await async_agent_service.GetTask(GetTaskRequest(task_type="async_dummy", resource_meta=metadata_bytes), ctx)
    assert res.resource.state == SUCCEEDED
    res = await async_agent_service.DeleteTask(
        DeleteTaskRequest(task_type="async_dummy", resource_meta=metadata_bytes), ctx
    )
    assert isinstance(res, DeleteTaskResponse)
    res = await sync_agent_service.DoTask(async_do_request, ctx)
    assert res.resource.state == SUCCEEDED

    res = await async_agent_service.GetTask(GetTaskRequest(task_type=fake_agent, resource_meta=metadata_bytes), ctx)
    assert res is None


def test_agent_server():
    loop.run_in_executor(None, run_agent_server)


def test_is_terminal_state():
    assert is_terminal_state(SUCCEEDED)
    assert is_terminal_state(RETRYABLE_FAILURE)
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


AgentRegistry.register(AsyncDummyAgent())
AgentRegistry.register(SyncDummyAgent())
