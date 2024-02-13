import json
import typing
from collections import OrderedDict
from dataclasses import asdict, dataclass
from unittest.mock import MagicMock, patch

import grpc
import pytest
from flyteidl.admin.agent_pb2 import (
    CreateRequestHeader,
    CreateTaskRequest,
    CreateTaskResponse,
    DeleteTaskRequest,
    DeleteTaskResponse,
    ExecuteTaskSyncRequest,
    ExecuteTaskSyncResponse,
    ExecuteTaskSyncResponseHeader,
    GetAgentRequest,
    GetTaskRequest,
    GetTaskResponse,
    ListAgentsRequest,
    ListAgentsResponse,
    Resource,
    TaskType,
)
from flyteidl.core.execution_pb2 import TaskExecution

from flytekit import FlyteContext, PythonFunctionTask, task
from flytekit.configuration import FastSerializationSettings, Image, ImageConfig, SerializationSettings
from flytekit.core.base_task import PythonTask, kwtypes
from flytekit.core.interface import Interface
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.agent_service import AgentMetadataService, AsyncAgentService, SyncAgentService
from flytekit.extend.backend.base_agent import (
    AgentRegistry,
    AsyncAgentBase,
    AsyncAgentExecutorMixin,
    SyncAgentBase,
    SyncAgentExecutorMixin,
    convert_to_flyte_phase,
    get_agent_secret,
    is_terminal_phase,
    render_task_template,
)
from flytekit.models import literals
from flytekit.models.core.execution import TaskLog
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.tools.translator import get_serializable

dummy_id = "dummy_id"


@dataclass
class Metadata:
    job_id: str


class DummyAgent(AsyncAgentBase):
    name = "Dummy Agent"

    def __init__(self):
        super().__init__(task_type_name="dummy")

    def create(
        self, output_prefix: str, task_template: TaskTemplate, inputs: typing.Optional[LiteralMap] = None, **kwargs
    ) -> CreateTaskResponse:
        return CreateTaskResponse(resource_meta=json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8"))

    def get(self, resource_meta: bytes, **kwargs) -> GetTaskResponse:
        return GetTaskResponse(
            resource=Resource(phase=TaskExecution.SUCCEEDED),
            log_links=[TaskLog(name="console", uri="localhost:3000").to_flyte_idl()],
        )

    def delete(self, resource_meta: bytes, **kwargs) -> DeleteTaskResponse:
        return DeleteTaskResponse()


class AsyncDummyAgent(AsyncAgentBase):
    name = "Async Dummy Agent"

    def __init__(self):
        super().__init__(task_type_name="async_dummy")

    async def create(
        self,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
        **kwargs,
    ) -> CreateTaskResponse:
        return CreateTaskResponse(resource_meta=json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8"))

    async def get(self, resource_meta: bytes, **kwargs) -> GetTaskResponse:
        return GetTaskResponse(resource=Resource(phase=TaskExecution.SUCCEEDED))

    async def delete(self, resource_meta: bytes, **kwargs) -> DeleteTaskResponse:
        return DeleteTaskResponse()


class MockOpenAIAgent(SyncAgentBase):
    name = "mock openAI Agent"

    def __init__(self):
        super().__init__(task_type_name="openai")

    def do(
        self,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Iterable[LiteralMap] = None,
        **kwargs,
    ) -> typing.Iterator[ExecuteTaskSyncResponse]:
        header = ExecuteTaskSyncResponseHeader(resource=Resource(phase=TaskExecution.SUCCEEDED))
        assert next(inputs).literals["a"].scalar.primitive.integer == 1
        ctx = FlyteContext.current_context()
        outputs = TypeEngine.dict_to_literal_map_idl(ctx, {"o0": 1})
        yield ExecuteTaskSyncResponse(header=header)
        yield ExecuteTaskSyncResponse(outputs=outputs)


class MockAsyncOpenAIAgent(SyncAgentBase):
    name = "mock async openAI Agent"

    def __init__(self):
        super().__init__(task_type_name="async_openai")

    async def do(
        self,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.AsyncIterator[LiteralMap] = None,
        **kwargs,
    ) -> typing.Iterator[ExecuteTaskSyncResponse]:
        header = ExecuteTaskSyncResponseHeader(resource=Resource(phase=TaskExecution.SUCCEEDED))
        i = await inputs.__anext__()
        assert i.literals["a"].scalar.primitive.integer == 1
        ctx = FlyteContext.current_context()
        out1 = TypeEngine.dict_to_literal_map_idl(ctx, {"o0": 1})
        out2 = TypeEngine.dict_to_literal_map_idl(ctx, {"o0": 2})
        yield ExecuteTaskSyncResponse(header=header)
        yield ExecuteTaskSyncResponse(outputs=out1)
        yield ExecuteTaskSyncResponse(outputs=out2)


def get_task_template(task_type: str) -> TaskTemplate:
    @task
    def simple_task(i: int):
        print(i)

    default_img = Image(name="default", fqn="test", tag="tag")
    serialization_settings = SerializationSettings(
        project="project",
        domain="domain",
        version="version",
        env={"FOO": "baz"},
        image_config=ImageConfig(default_image=default_img, images=[default_img]),
        fast_serialization_settings=FastSerializationSettings(enabled=True),
    )
    serialized = get_serializable(OrderedDict(), serialization_settings, simple_task)
    serialized.template._type = task_type
    return serialized.template


task_inputs = literals.LiteralMap(
    {
        "a": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(integer=1))),
    },
)


def test_dummy_agent():
    AgentRegistry.register(DummyAgent(), override=True)
    agent = AgentRegistry.get_agent("dummy")
    template = get_task_template("dummy")
    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")
    assert agent.create("/tmp", template, task_inputs).resource_meta == metadata_bytes
    res = agent.get(metadata_bytes)
    assert res.resource.phase == TaskExecution.SUCCEEDED
    assert res.log_links[0].name == "console"
    assert res.log_links[0].uri == "localhost:3000"
    assert agent.delete(metadata_bytes) == DeleteTaskResponse()

    class DummyTask(AsyncAgentExecutorMixin, PythonFunctionTask):
        def __init__(self, **kwargs):
            super().__init__(task_type="dummy", **kwargs)

    t = DummyTask(task_config={}, task_function=lambda: None, container_image="dummy")
    t.execute()

    t._task_type = "non-exist-type"
    with pytest.raises(Exception, match="Cannot find agent for task type: non-exist-type."):
        t.execute()


@pytest.mark.parametrize("agent", [DummyAgent(), AsyncDummyAgent()], ids=["sync", "async"])
@pytest.mark.asyncio
async def test_async_agent_service(agent):
    AgentRegistry.register(agent, override=True)
    service = AsyncAgentService()
    ctx = MagicMock(spec=grpc.ServicerContext)

    inputs_proto = task_inputs.to_flyte_idl()
    output_prefix = "/tmp"
    metadata_bytes = json.dumps(asdict(Metadata(job_id=dummy_id))).encode("utf-8")

    tmp = get_task_template(agent.task_type_name).to_flyte_idl()
    task_type = TaskType(name=agent.task_type_name)
    req = CreateTaskRequest(inputs=inputs_proto, output_prefix=output_prefix, template=tmp)

    res = await service.CreateTask(req, ctx)
    assert res.resource_meta == metadata_bytes
    res = await service.GetTask(GetTaskRequest(task_type=task_type, resource_meta=metadata_bytes), ctx)
    assert res.resource.phase == TaskExecution.SUCCEEDED
    res = await service.DeleteTask(DeleteTaskRequest(task_type=task_type, resource_meta=metadata_bytes), ctx)
    assert isinstance(res, DeleteTaskResponse)

    agent_metadata = AgentRegistry.get_agent_metadata(agent.name)
    assert agent_metadata.supported_task_types[0].name == agent.task_type_name


def test_register_agent():
    agent = DummyAgent()
    AgentRegistry.register(agent, override=True)
    assert AgentRegistry.get_agent("dummy").name == agent.name

    with pytest.raises(ValueError, match="Duplicate agent for task type: dummy, version: 0"):
        AgentRegistry.register(agent)


@pytest.mark.asyncio
async def test_agent_metadata_service():
    agent = DummyAgent()
    AgentRegistry.register(agent, override=True)

    ctx = MagicMock(spec=grpc.ServicerContext)
    metadata_service = AgentMetadataService()
    res = await metadata_service.ListAgents(ListAgentsRequest(), ctx)
    assert isinstance(res, ListAgentsResponse)
    res = await metadata_service.GetAgent(GetAgentRequest(task_type=TaskType(name="dummy")), ctx)
    assert res.agent.name == agent.name
    assert res.agent.supported_task_types[0].name == agent.task_type_name


def test_openai_agent():
    AgentRegistry.register(MockOpenAIAgent(), override=True)

    class OpenAITask(SyncAgentExecutorMixin, PythonTask):
        def __init__(self, **kwargs):
            super().__init__(
                task_type="openai", interface=Interface(inputs=kwtypes(a=int), outputs=kwtypes(o0=int)), **kwargs
            )

    t = OpenAITask(task_config={}, name="openai task")
    res = t(a=1)
    assert res == 1


def test_async_openai_agent():
    AgentRegistry.register(MockAsyncOpenAIAgent(), override=True)

    class OpenAITask(SyncAgentExecutorMixin, PythonTask):
        def __init__(self, **kwargs):
            super().__init__(
                task_type="async_openai",
                interface=Interface(inputs=kwtypes(a=int), outputs=kwtypes(o0=typing.List[int])),
                **kwargs,
            )

    t = OpenAITask(task_config={}, name="openai task")
    res = t(a=1)
    assert res == [1, 2]


async def get_request_iterator(task_type: str):
    inputs_proto = task_inputs.to_flyte_idl()
    template = get_task_template(task_type).to_flyte_idl()
    header = CreateRequestHeader(template=template, output_prefix="/tmp")
    yield ExecuteTaskSyncRequest(header=header)
    yield ExecuteTaskSyncRequest(inputs=inputs_proto)


@pytest.mark.asyncio
async def test_sync_agent_service():
    AgentRegistry.register(MockOpenAIAgent(), override=True)
    ctx = MagicMock(spec=grpc.ServicerContext)

    service = SyncAgentService()
    res_iter = await service.ExecuteTaskSync(get_request_iterator("openai"), ctx)
    res = next(res_iter)
    assert res.header.resource.phase == TaskExecution.SUCCEEDED
    res = next(res_iter)
    assert res.outputs.literals["o0"].scalar.primitive.integer == 1


@pytest.mark.asyncio
async def test_sync_agent_service_with_asyncio():
    AgentRegistry.register(MockAsyncOpenAIAgent(), override=True)
    AgentRegistry.register(DummyAgent(), override=True)
    ctx = MagicMock(spec=grpc.ServicerContext)

    service = SyncAgentService()
    res_iter = await service.ExecuteTaskSync(get_request_iterator("async_openai"), ctx)
    res = await res_iter.__anext__()
    assert res.header.resource.phase == TaskExecution.SUCCEEDED
    res = await res_iter.__anext__()
    assert res.outputs.literals["o0"].scalar.primitive.integer == 1
    res = await res_iter.__anext__()
    assert res.outputs.literals["o0"].scalar.primitive.integer == 2

    res_iter = await service.ExecuteTaskSync(get_request_iterator("dummy"), ctx)
    assert res_iter is None


def test_is_terminal_phase():
    assert is_terminal_phase(TaskExecution.SUCCEEDED)
    assert is_terminal_phase(TaskExecution.ABORTED)
    assert is_terminal_phase(TaskExecution.FAILED)
    assert not is_terminal_phase(TaskExecution.RUNNING)


def test_convert_to_flyte_phase():
    assert convert_to_flyte_phase("FAILED") == TaskExecution.FAILED
    assert convert_to_flyte_phase("TIMEOUT") == TaskExecution.FAILED
    assert convert_to_flyte_phase("TIMEDOUT") == TaskExecution.FAILED
    assert convert_to_flyte_phase("CANCELED") == TaskExecution.FAILED

    assert convert_to_flyte_phase("DONE") == TaskExecution.SUCCEEDED
    assert convert_to_flyte_phase("SUCCEEDED") == TaskExecution.SUCCEEDED
    assert convert_to_flyte_phase("SUCCESS") == TaskExecution.SUCCEEDED

    assert convert_to_flyte_phase("RUNNING") == TaskExecution.RUNNING

    invalid_state = "INVALID_STATE"
    with pytest.raises(Exception, match=f"Unrecognized state: {invalid_state.lower()}"):
        convert_to_flyte_phase(invalid_state)


@patch("flytekit.current_context")
def test_get_agent_secret(mocked_context):
    mocked_context.return_value.secrets.get.return_value = "mocked token"
    assert get_agent_secret("mocked key") == "mocked token"


def test_render_task_template():
    template = get_task_template("dummy")
    tt = render_task_template(template, "s3://becket")
    assert tt.container.args == [
        "pyflyte-fast-execute",
        "--additional-distribution",
        "{{ .remote_package_path }}",
        "--dest-dir",
        "{{ .dest_dir }}",
        "--",
        "pyflyte-execute",
        "--inputs",
        "s3://becket/inputs.pb",
        "--output-prefix",
        "s3://becket",
        "--raw-output-data-prefix",
        "s3://becket/raw_output",
        "--checkpoint-path",
        "s3://becket/checkpoint_output",
        "--prev-checkpoint",
        "s3://becket/prev_checkpoint",
        "--resolver",
        "flytekit.core.python_auto_container.default_task_resolver",
        "--",
        "task-module",
        "test_agent",
        "task-name",
        "simple_task",
    ]
