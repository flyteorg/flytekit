import typing
from collections import OrderedDict
from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import grpc
import pytest
from flyteidl.admin.agent_pb2 import (
    Agent,
    CreateRequestHeader,
    CreateTaskRequest,
    DeleteTaskRequest,
    ExecuteTaskSyncRequest,
    GetAgentRequest,
    GetTaskRequest,
    ListAgentsRequest,
    ListAgentsResponse,
    TaskCategory, DeleteTaskResponse, GetTaskLogsResponse, GetTaskLogsResponseBody, GetTaskMetricsResponse,
    GetTaskMetricsRequest, GetTaskLogsRequest,
)
from flyteidl.core.execution_pb2 import TaskExecution, TaskLog
from flyteidl.core.identifier_pb2 import ResourceType
from flyteidl.core.metrics_pb2 import ExecutionMetricResult

from flytekit import PythonFunctionTask, task
from flytekit.configuration import (
    FastSerializationSettings,
    Image,
    ImageConfig,
    SerializationSettings,
)
from flytekit.core.base_task import PythonTask, kwtypes
from flytekit.core.interface import Interface
from flytekit.exceptions.system import FlyteConnectorNotFound
from flytekit.extend.backend.connector_service import (
    ConnectorMetadataService,
    AsyncConnectorService,
    SyncConnectorService,
)
from flytekit.extend.backend.base_connector import (
    ConnectorRegistry,
    AsyncConnectorBase,
    AsyncConnectorExecutorMixin,
    Resource,
    ResourceMeta,
    SyncConnectorBase,
    SyncConnectorExecutorMixin,
    is_terminal_phase,
    render_task_template,
)
from flytekit.extend.backend.utils import convert_to_flyte_phase, get_agent_secret, get_connector_secret
from flytekit.models import literals
from flytekit.models.core.identifier import (
    Identifier,
    NodeExecutionIdentifier,
    TaskExecutionIdentifier,
    WorkflowExecutionIdentifier,
)
from flytekit.models.literals import LiteralMap
from flytekit.models.security import Identity
from flytekit.models.task import TaskExecutionMetadata, TaskTemplate
from flytekit.tools.translator import get_serializable
from flytekit.utils.asyn import loop_manager

dummy_id = "dummy_id"


@dataclass
class DummyMetadata(ResourceMeta):
    job_id: str
    output_path: typing.Optional[str] = None
    task_name: typing.Optional[str] = None


class DummyConnector(AsyncConnectorBase):
    name = "Dummy Connector"

    def __init__(self):
        super().__init__(task_type_name="dummy", metadata_type=DummyMetadata)

    def create(self, task_template: TaskTemplate, inputs: typing.Optional[LiteralMap], **kwargs) -> DummyMetadata:
        return DummyMetadata(job_id=dummy_id)

    def get(self, resource_meta: DummyMetadata, **kwargs) -> Resource:
        return Resource(
            phase=TaskExecution.SUCCEEDED,
            log_links=[TaskLog(name="console", uri="localhost:3000")],
            custom_info={"custom": "info", "num": 1},
        )

    def delete(self, resource_meta: DummyMetadata, **kwargs):
        ...


class AsyncDummyConnector(AsyncConnectorBase):
    name = "Async Dummy Connector"

    def __init__(self):
        super().__init__(task_type_name="async_dummy", metadata_type=DummyMetadata)

    async def create(
        self,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
        output_prefix: typing.Optional[str] = None,
        task_execution_metadata: typing.Optional[TaskExecutionMetadata] = None,
        **kwargs,
    ) -> DummyMetadata:
        output_path = f"{output_prefix}/{dummy_id}" if output_prefix else None
        task_name = task_execution_metadata.task_execution_id.task_id.name if task_execution_metadata else "default"
        return DummyMetadata(job_id=dummy_id, output_path=output_path, task_name=task_name)

    async def get(self, resource_meta: DummyMetadata, **kwargs) -> Resource:
        return Resource(
            phase=TaskExecution.SUCCEEDED,
            log_links=[TaskLog(name="console", uri="localhost:3000")],
            custom_info={"custom": "info", "num": 1},
        )

    async def delete(self, resource_meta: DummyMetadata, **kwargs):
        ...

    async def get_metrics(self, resource_meta: DummyMetadata, **kwargs) -> GetTaskMetricsResponse:
        return GetTaskMetricsResponse(results=[ExecutionMetricResult(metric="EXECUTION_METRIC_LIMIT_MEMORY_BYTES", data=None)])

    async def get_logs(self, resource_meta: DummyMetadata, **kwargs) -> GetTaskLogsResponse:
        return GetTaskLogsResponse(body=GetTaskLogsResponseBody(results=["foo", "bar"]))


class MockOpenAIConnector(SyncConnectorBase):
    name = "mock openAI Connector"

    def __init__(self):
        super().__init__(task_type_name="openai")

    def do(
        self,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
        **kwargs,
    ) -> Resource:
        assert inputs.literals["a"].scalar.primitive.integer == 1
        return Resource(phase=TaskExecution.SUCCEEDED, outputs={"o0": 1})


class MockAsyncOpenAIConnector(SyncConnectorBase):
    name = "mock async openAI Connector"

    def __init__(self):
        super().__init__(task_type_name="async_openai")

    async def do(self, task_template: TaskTemplate, inputs: LiteralMap = None, **kwargs) -> Resource:
        assert inputs.literals["a"].scalar.primitive.integer == 1
        return Resource(phase=TaskExecution.SUCCEEDED, outputs={"o0": 1})


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

task_execution_metadata = TaskExecutionMetadata(
    task_execution_id=TaskExecutionIdentifier(
        task_id=Identifier(ResourceType.TASK, "project", "domain", "name", "version"),
        node_execution_id=NodeExecutionIdentifier("node_id", WorkflowExecutionIdentifier("project", "domain", "name")),
        retry_attempt=1,
    ),
    namespace="namespace",
    labels={"label_key": "label_val"},
    annotations={"annotation_key": "annotation_val"},
    k8s_service_account="k8s service account",
    environment_variables={"env_var_key": "env_var_val"},
    identity=Identity(execution_identity="task executor"),
)


def test_dummy_connector():
    ConnectorRegistry.register(DummyConnector(), override=True)
    connector = ConnectorRegistry.get_connector("dummy")
    template = get_task_template("dummy")
    metadata = DummyMetadata(job_id=dummy_id)
    assert connector.create(template, task_inputs) == DummyMetadata(job_id=dummy_id)
    resource = connector.get(metadata)
    assert resource.phase == TaskExecution.SUCCEEDED
    assert resource.log_links[0].name == "console"
    assert resource.log_links[0].uri == "localhost:3000"
    assert resource.custom_info["custom"] == "info"
    assert resource.custom_info["num"] == 1
    assert connector.delete(metadata) is None

    class DummyTask(AsyncConnectorExecutorMixin, PythonFunctionTask):
        def __init__(self, **kwargs):
            super().__init__(task_type="dummy", **kwargs)

    t = DummyTask(task_config={}, task_function=lambda: None, container_image="dummy")
    t.execute()

    t._task_type = "non-exist-type"
    with pytest.raises(Exception, match="Cannot find connector for task category: non-exist-type."):
        t.execute()


@pytest.mark.parametrize(
    "connector,consume_metadata",
    [(DummyConnector(), False), (AsyncDummyConnector(), True)],
    ids=["sync", "async"],
)
@pytest.mark.asyncio
async def test_async_connector_service(connector, consume_metadata):
    ConnectorRegistry.register(connector, override=True)
    service = AsyncConnectorService()
    ctx = MagicMock(spec=grpc.ServicerContext)

    inputs_proto = task_inputs.to_flyte_idl()
    output_prefix = "/tmp"
    metadata_bytes = (
        DummyMetadata(
            job_id=dummy_id,
            output_path=f"{output_prefix}/{dummy_id}",
            task_name=task_execution_metadata.task_execution_id.task_id.name,
        ).encode()
        if consume_metadata
        else DummyMetadata(job_id=dummy_id).encode()
    )

    tmp = get_task_template(connector.task_category.name).to_flyte_idl()
    task_category = TaskCategory(name=connector.task_category.name, version=0)
    req = CreateTaskRequest(
        inputs=inputs_proto,
        template=tmp,
        output_prefix=output_prefix,
        task_execution_metadata=task_execution_metadata.to_flyte_idl(),
    )

    res = await service.CreateTask(req, ctx)
    assert res.resource_meta == metadata_bytes
    res = await service.GetTask(GetTaskRequest(task_category=task_category, resource_meta=metadata_bytes), ctx)
    assert res.resource.phase == TaskExecution.SUCCEEDED
    res = await service.DeleteTask(
        DeleteTaskRequest(task_category=task_category, resource_meta=metadata_bytes),
        ctx,
    )
    assert res == DeleteTaskResponse()
    if connector.task_category.name == "async_dummy":
        res = await service.GetTaskMetrics(GetTaskMetricsRequest(task_category=task_category, resource_meta=metadata_bytes), ctx)
        assert res.results[0].metric == "EXECUTION_METRIC_LIMIT_MEMORY_BYTES"

        res = await service.GetTaskLogs(GetTaskLogsRequest(task_category=task_category, resource_meta=metadata_bytes), ctx)
        assert res.body.results == ["foo", "bar"]


    connector_metadata = ConnectorRegistry.get_connector_metadata(connector.name)
    assert connector_metadata.supported_task_types[0] == connector.task_category.name
    assert connector_metadata.supported_task_categories[0].name == connector.task_category.name

    with pytest.raises(FlyteConnectorNotFound):
        ConnectorRegistry.get_connector_metadata("non-exist-namr")


def test_register_connector():
    connector = DummyConnector()
    ConnectorRegistry.register(connector, override=True)
    assert ConnectorRegistry.get_connector("dummy").name == connector.name

    with pytest.raises(ValueError, match="Duplicate connector for task type: dummy_v0"):
        ConnectorRegistry.register(connector)

    with pytest.raises(FlyteConnectorNotFound):
        ConnectorRegistry.get_connector("non-exist-type")

    connectors = ConnectorRegistry.list_connectors()
    assert len(connectors) >= 1


@pytest.mark.asyncio
async def test_connector_metadata_service():
    connector = DummyConnector()
    ConnectorRegistry.register(connector, override=True)

    ctx = MagicMock(spec=grpc.ServicerContext)
    metadata_service = ConnectorMetadataService()
    res = await metadata_service.ListAgents(ListAgentsRequest(), ctx)
    assert isinstance(res, ListAgentsResponse)
    res = await metadata_service.GetAgent(GetAgentRequest(name="Dummy Connector"), ctx)
    assert res.agent.name == connector.name
    assert res.agent.supported_task_types[0] == connector.task_category.name
    assert res.agent.supported_task_categories[0].name == connector.task_category.name


def test_openai_connector():
    ConnectorRegistry.register(MockOpenAIConnector(), override=True)

    class OpenAITask(SyncConnectorExecutorMixin, PythonTask):
        def __init__(self, **kwargs):
            super().__init__(
                task_type="openai",
                interface=Interface(inputs=kwtypes(a=int), outputs=kwtypes(o0=int)),
                **kwargs,
            )

    t = OpenAITask(task_config={}, name="openai task")
    res = t(a=1)
    assert res == 1


def test_async_openai_connector():
    ConnectorRegistry.register(MockAsyncOpenAIConnector(), override=True)

    class OpenAITask(SyncConnectorExecutorMixin, PythonTask):
        def __init__(self, **kwargs):
            super().__init__(
                task_type="async_openai",
                interface=Interface(inputs=kwtypes(a=int), outputs=kwtypes(o0=int)),
                **kwargs,
            )

    t = OpenAITask(task_config={}, name="openai task")
    res = t(a=1)
    assert res == 1


async def get_request_iterator(task_type: str):
    inputs_proto = task_inputs.to_flyte_idl()
    template = get_task_template(task_type).to_flyte_idl()
    header = CreateRequestHeader(template=template, output_prefix="/tmp")
    yield ExecuteTaskSyncRequest(header=header)
    yield ExecuteTaskSyncRequest(inputs=inputs_proto)


@pytest.mark.asyncio
async def test_sync_connector_service():
    ConnectorRegistry.register(MockOpenAIConnector(), override=True)
    ctx = MagicMock(spec=grpc.ServicerContext)

    service = SyncConnectorService()
    res = await service.ExecuteTaskSync(get_request_iterator("openai"), ctx).__anext__()
    assert res.header.resource.phase == TaskExecution.SUCCEEDED
    assert res.header.resource.outputs.literals["o0"].scalar.primitive.integer == 1


@pytest.mark.asyncio
async def test_sync_connector_service_with_asyncio():
    ConnectorRegistry.register(MockAsyncOpenAIConnector(), override=True)
    ConnectorRegistry.register(DummyConnector(), override=True)
    ctx = MagicMock(spec=grpc.ServicerContext)

    service = SyncConnectorService()
    res = await service.ExecuteTaskSync(get_request_iterator("async_openai"), ctx).__anext__()
    assert res.header.resource.phase == TaskExecution.SUCCEEDED
    assert res.header.resource.outputs.literals["o0"].scalar.primitive.integer == 1


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
    assert convert_to_flyte_phase("SKIPPED") == TaskExecution.FAILED
    assert convert_to_flyte_phase("INTERNAL_ERROR") == TaskExecution.FAILED

    assert convert_to_flyte_phase("DONE") == TaskExecution.SUCCEEDED
    assert convert_to_flyte_phase("SUCCEEDED") == TaskExecution.SUCCEEDED
    assert convert_to_flyte_phase("SUCCESS") == TaskExecution.SUCCEEDED

    assert convert_to_flyte_phase("RUNNING") == TaskExecution.RUNNING
    assert convert_to_flyte_phase("TERMINATING") == TaskExecution.RUNNING

    assert convert_to_flyte_phase("PENDING") == TaskExecution.INITIALIZING

    invalid_state = "INVALID_STATE"
    with pytest.raises(Exception, match=f"Unrecognized state: {invalid_state.lower()}"):
        convert_to_flyte_phase(invalid_state)


@patch("flytekit.current_context")
def test_get_connector_secret(mocked_context):
    mocked_context.return_value.secrets.get.return_value = "mocked token"
    assert get_agent_secret("mocked key") == "mocked token"
    assert get_connector_secret("mocked key") == "mocked token"


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
        "test_connector",
        "task-name",
        "simple_task",
    ]


@pytest.fixture
def sample_connectors():
    async_connector = Agent(
        name="Sensor",
        is_sync=False,
        supported_task_categories=[TaskCategory(name="sensor", version=0)],
    )
    sync_connector = Agent(
        name="ChatGPT Agent",
        is_sync=True,
        supported_task_categories=[TaskCategory(name="chatgpt", version=0)],
    )
    return [async_connector, sync_connector]


def test_resource_type():
    o = Resource(
        phase=TaskExecution.SUCCEEDED,
    )
    v = loop_manager.run_sync(o.to_flyte_idl)
    assert v
    assert v.phase == TaskExecution.SUCCEEDED
    assert len(v.log_links) == 0
    assert v.message == ""
    assert len(v.outputs.literals) == 0
    assert len(v.custom_info) == 0

    o2 = Resource.from_flyte_idl(v)
    assert o2

    o = Resource(
        phase=TaskExecution.SUCCEEDED,
        log_links=[TaskLog(name="console", uri="localhost:3000")],
        message="foo",
        outputs={"o0": 1},
        custom_info={"custom": "info", "num": 1},
    )
    v = loop_manager.run_sync(o.to_flyte_idl)
    assert v
    assert v.phase == TaskExecution.SUCCEEDED
    assert v.log_links[0].name == "console"
    assert v.log_links[0].uri == "localhost:3000"
    assert v.message == "foo"
    assert v.outputs.literals["o0"].scalar.primitive.integer == 1
    assert v.custom_info["custom"] == "info"
    assert v.custom_info["num"] == 1

    o2 = Resource.from_flyte_idl(v)
    assert o2.phase == o.phase
    assert list(o2.log_links) == list(o.log_links)
    assert o2.message == o.message
    # round-tripping creates a literal map out of outputs
    assert o2.outputs.literals["o0"].scalar.primitive.integer == 1
    assert o2.custom_info == o.custom_info


def test_connector_complex_type():
    @dataclass
    class Foo:
        val: str

    class FooConnector(SyncConnectorBase):
        def __init__(self) -> None:
            super().__init__(task_type_name="foo")

        def do(
                self,
                task_template: TaskTemplate,
                inputs: typing.Optional[LiteralMap] = None,
                **kwargs: typing.Any,
        ) -> Resource:
            return Resource(
                phase=TaskExecution.SUCCEEDED, outputs={"foos": [Foo(val="a"), Foo(val="b")], "has_foos": True}
            )

    ConnectorRegistry.register(FooConnector(), override=True)

    class FooTask(SyncConnectorExecutorMixin, PythonTask):  # type: ignore
        _TASK_TYPE = "foo"

        def __init__(self, name: str, **kwargs: typing.Any) -> None:
            task_config: dict[str, typing.Any] = {}

            outputs = {"has_foos": bool, "foos": typing.Optional[typing.List[Foo]]}

            super().__init__(
                task_type=self._TASK_TYPE,
                name=name,
                task_config=task_config,
                interface=Interface(outputs=outputs),
                **kwargs,
            )

        def get_custom(self, settings: SerializationSettings) -> typing.Dict[str, typing.Any]:
            return {}

    foo_task = FooTask(name="foo_task")
    res = foo_task()
    assert res.has_foos
    assert res.foos[1].val == "b"
