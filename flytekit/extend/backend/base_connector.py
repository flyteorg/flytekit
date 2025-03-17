import asyncio
import inspect
import json
import sys
import time
import typing
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import asdict, dataclass
from functools import partial
from types import FrameType
from typing import Any, Dict, List, Optional, Union

from flyteidl.admin.agent_pb2 import Agent, GetTaskLogsResponse, GetTaskMetricsResponse
from flyteidl.admin.agent_pb2 import Resource as _Resource
from flyteidl.admin.agent_pb2 import TaskCategory as _TaskCategory
from flyteidl.core import literals_pb2
from flyteidl.core.execution_pb2 import TaskExecution, TaskLog
from google.protobuf import json_format
from google.protobuf.struct_pb2 import Struct
from rich.logging import RichHandler
from rich.progress import Progress

from flytekit import FlyteContext, PythonFunctionTask, logger
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core import utils
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import ExecutionState, FlyteContextManager
from flytekit.core.type_engine import TypeEngine, dataclass_from_dict
from flytekit.exceptions.system import FlyteConnectorNotFound
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend.backend.utils import is_terminal_phase, mirror_async_methods, render_task_template
from flytekit.loggers import set_flytekit_log_properties
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskExecutionMetadata, TaskTemplate

# It's used to force connector to run in the same event loop in the local execution.
local_connector_loop = asyncio.new_event_loop()


class TaskCategory:
    def __init__(self, name: str, version: int = 0):
        self._name = name
        self._version = version

    def __hash__(self):
        return hash((self._name, self._version))

    def __eq__(self, other: "TaskCategory"):
        return self._name == other._name and self._version == other._version

    @property
    def name(self) -> str:
        return self._name

    @property
    def version(self) -> int:
        return self._version

    def __str__(self):
        return f"{self._name}_v{self._version}"


@dataclass
class ResourceMeta:
    """
    This is the metadata for the job. For example, the id of the job.
    """

    def encode(self) -> bytes:
        """
        Encode the resource meta to bytes.
        """
        return json.dumps(asdict(self)).encode("utf-8")

    @classmethod
    def decode(cls, data: bytes) -> "ResourceMeta":
        """
        Decode the resource meta from bytes.
        """
        return dataclass_from_dict(cls, json.loads(data.decode("utf-8")))


@dataclass
class Resource:
    """
    This is the output resource of the job.

    Attributes
    ----------
        phase : TaskExecution.Phase
            The phase of the job.
        message : Optional[str]
            The return message from the job.
        log_links : Optional[List[TaskLog]]
            The log links of the job. For example, the link to the BigQuery Console.
        outputs : Optional[Union[LiteralMap, typing.Dict[str, Any]]]
            The outputs of the job. If return python native types, the agent will convert them to flyte literals.
        custom_info : Optional[typing.Dict[str, Any]]
            The custom info of the job. For example, the job config.
    """

    phase: TaskExecution.Phase
    message: Optional[str] = None
    log_links: Optional[List[TaskLog]] = None
    outputs: Optional[Union[LiteralMap, typing.Dict[str, Any]]] = None
    custom_info: Optional[typing.Dict[str, Any]] = None

    async def to_flyte_idl(self) -> _Resource:
        """
        This function is async to call the async type engine functions. This is okay to do because this is not a
        normal model class that inherits from FlyteIdlEntity
        """
        if self.outputs is None:
            outputs = None
        elif isinstance(self.outputs, LiteralMap):
            outputs = self.outputs.to_flyte_idl()
        else:
            ctx = FlyteContext.current_context()

            outputs = await TypeEngine.dict_to_literal_map_pb(ctx, self.outputs)

        return _Resource(
            phase=self.phase,
            message=self.message,
            log_links=self.log_links,
            outputs=outputs,
            custom_info=(json_format.Parse(json.dumps(self.custom_info), Struct()) if self.custom_info else None),
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object: _Resource):
        return cls(
            phase=pb2_object.phase,
            message=pb2_object.message,
            log_links=pb2_object.log_links,
            outputs=(LiteralMap.from_flyte_idl(pb2_object.outputs) if pb2_object.outputs else None),
            custom_info=(
                json_format.MessageToDict(pb2_object.custom_info) if pb2_object.HasField("custom_info") else None
            ),
        )


class ConnectorBase(ABC):
    name = "Base Connector"

    def __init__(self, task_type_name: str, task_type_version: int = 0, **kwargs):
        self._task_category = TaskCategory(name=task_type_name, version=task_type_version)

    @property
    def task_category(self) -> TaskCategory:
        """
        task category that the connector supports
        """
        return self._task_category


class SyncConnectorBase(ConnectorBase):
    """
    This is the base class for all sync connectors.
    It defines the interface that all connectors must implement.
    The connector service is responsible for invoking connectors.
    Propeller sends a request to connector service, and gets a response in the same call.

    All the connectors should be registered in the ConnectorRegistry.
    Connector Service
    will look up the connector based on the task type. Every task type can only have one connector.
    """

    name = "Base Sync Connector"

    @abstractmethod
    def do(
        self, task_template: TaskTemplate, output_prefix: str, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> Resource:
        """
        This is the method that the connector will run.
        """
        raise NotImplementedError


class AsyncConnectorBase(ConnectorBase):
    """
    This is the base class for all async connectors.
    It defines the interface that all connectors must implement.
    The connector service is responsible for invoking connectors. The propeller will communicate with the connector service
    to create tasks, get the status of tasks, and delete tasks.

    All the connectors should be registered in the ConnectorRegistry.
    Connector Service
    will look up the connector based on the task type. Every task type can only have one connector.
    """

    name = "Base Async Connector"

    def __init__(self, metadata_type: ResourceMeta, **kwargs):
        super().__init__(**kwargs)
        self._metadata_type = metadata_type

    @property
    def metadata_type(self) -> ResourceMeta:
        return self._metadata_type

    @abstractmethod
    def create(
        self,
        task_template: TaskTemplate,
        output_prefix: str,
        inputs: Optional[LiteralMap],
        task_execution_metadata: Optional[TaskExecutionMetadata],
        **kwargs,
    ) -> ResourceMeta:
        """
        Return a resource meta that can be used to get the status of the task.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, resource_meta: ResourceMeta, **kwargs) -> Resource:
        """
        Return the status of the task, and return the outputs in some cases. For example, bigquery job
        can't write the structured dataset to the output location, so it returns the output literals to the propeller,
        and the propeller will write the structured dataset to the blob store.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, resource_meta: ResourceMeta, **kwargs):
        """
        Delete the task. This call should be idempotent. It should raise an error if fails to delete the task.
        """
        raise NotImplementedError

    def get_metrics(self, resource_meta: ResourceMeta, **kwargs) -> GetTaskMetricsResponse:
        """
        Return the metrics for the task.
        """
        raise NotImplementedError

    def get_logs(self, resource_meta: ResourceMeta, **kwargs) -> GetTaskLogsResponse:
        """
        Return the metrics for the task.
        """
        raise NotImplementedError


class ConnectorRegistry(object):
    """
    This is the registry for all connectors.
    The connector service will look up the connector registry based on the task type.
    The connector metadata service will look up the connector metadata based on the connector name.
    """

    _REGISTRY: Dict[TaskCategory, Union[AsyncConnectorBase, SyncConnectorBase]] = {}
    _METADATA: Dict[str, Agent] = {}

    @staticmethod
    def register(connector: Union[AsyncConnectorBase, SyncConnectorBase], override: bool = False):
        if connector.task_category in ConnectorRegistry._REGISTRY and override is False:
            raise ValueError(f"Duplicate connector for task type: {connector.task_category}")
        ConnectorRegistry._REGISTRY[connector.task_category] = connector

        task_category = _TaskCategory(name=connector.task_category.name, version=connector.task_category.version)

        if connector.name in ConnectorRegistry._METADATA:
            connector_metadata = ConnectorRegistry.get_connector_metadata(connector.name)
            connector_metadata.supported_task_categories.append(task_category)
            connector_metadata.supported_task_types.append(task_category.name)
        else:
            connector_metadata = Agent(
                name=connector.name,
                supported_task_types=[task_category.name],
                supported_task_categories=[task_category],
                is_sync=isinstance(connector, SyncConnectorBase),
            )
            ConnectorRegistry._METADATA[connector.name] = connector_metadata

    @staticmethod
    def get_connector(task_type_name: str, task_type_version: int = 0) -> Union[SyncConnectorBase, AsyncConnectorBase]:
        task_category = TaskCategory(name=task_type_name, version=task_type_version)
        if task_category not in ConnectorRegistry._REGISTRY:
            raise FlyteConnectorNotFound(f"Cannot find connector for task category: {task_category}.")
        return ConnectorRegistry._REGISTRY[task_category]

    @staticmethod
    def get_agent(task_type_name: str, task_type_version: int = 0) -> Union[SyncConnectorBase, AsyncConnectorBase]:
        # Keep this function for backward compatibility.
        task_category = TaskCategory(name=task_type_name, version=task_type_version)
        if task_category not in ConnectorRegistry._REGISTRY:
            raise FlyteConnectorNotFound(f"Cannot find connector for task category: {task_category}.")
        return ConnectorRegistry._REGISTRY[task_category]

    @staticmethod
    def list_connectors() -> List[Agent]:
        return list(ConnectorRegistry._METADATA.values())

    @staticmethod
    def get_connector_metadata(name: str) -> Agent:
        if name not in ConnectorRegistry._METADATA:
            raise FlyteConnectorNotFound(f"Cannot find connector for name: {name}.")
        return ConnectorRegistry._METADATA[name]


class SyncConnectorExecutorMixin:
    """
    This mixin class is used to run the sync task locally, and it's only used for local execution.
    Task should inherit from this class if the task can be run in the connector.

    Synchronous tasks run quickly and can return their results instantly.
    Sending a prompt to ChatGPT and getting a response, or retrieving some metadata from a backend system.
    """

    T = typing.TypeVar("T", PythonTask, "SyncConnectorExecutorMixin")

    def execute(self: T, **kwargs) -> LiteralMap:
        from flytekit.tools.translator import get_serializable

        ctx = FlyteContext.current_context()
        ss = ctx.serialization_settings or SerializationSettings(ImageConfig())
        task_template = get_serializable(OrderedDict(), ss, self).template
        if task_template.metadata.timeout:
            logger.info("Timeout is not supported for local execution.\n" "Ignoring the timeout.")
        output_prefix = ctx.file_access.get_random_remote_directory()

        connector = ConnectorRegistry.get_connector(task_template.type, task_template.task_type_version)
        resource = local_connector_loop.run_until_complete(
            self._do(connector=connector, template=task_template, output_prefix=output_prefix, inputs=kwargs)
        )
        if resource.phase != TaskExecution.SUCCEEDED:
            raise FlyteUserException(f"Failed to run the task {self.name} with error: {resource.message}")

        if resource.outputs and not isinstance(resource.outputs, LiteralMap):
            return TypeEngine.dict_to_literal_map(ctx, resource.outputs, type_hints=self.python_interface.outputs)
        return resource.outputs

    async def _do(
        self: T,
        connector: SyncConnectorBase,
        template: TaskTemplate,
        output_prefix: str,
        inputs: Dict[str, Any] = None,
    ) -> Resource:
        try:
            ctx = FlyteContext.current_context()
            literal_map = await TypeEngine._dict_to_literal_map(ctx, inputs or {}, self.get_input_types())
            return await mirror_async_methods(
                connector.do, task_template=template, inputs=literal_map, output_prefix=output_prefix
            )
        except Exception as e:
            e.args = (f"Failed to run the task {self.name} with error: {e.args[0]}",)
            raise


class AsyncConnectorExecutorMixin:
    """
    This mixin class is used to run the async task locally, and it's only used for local execution.
    Task should inherit from this class if the task can be run in the connector.

    Asynchronous tasks are tasks that take a long time to complete, such as running a query.
    """

    T = typing.TypeVar("T", PythonTask, "AsyncConnectorExecutorMixin")

    _clean_up_task: bool = False
    _connector: AsyncConnectorBase = None
    resource_meta = None

    def execute(self: T, **kwargs) -> LiteralMap:
        ctx = FlyteContext.current_context()
        ss = ctx.serialization_settings or SerializationSettings(ImageConfig())
        output_prefix = ctx.file_access.get_random_remote_directory()
        from flytekit.tools.translator import get_serializable

        task_template = get_serializable(OrderedDict(), ss, self).template
        if task_template.metadata.timeout:
            logger.info("Timeout is not supported for local execution.\n" "Ignoring the timeout.")
        self._connector = ConnectorRegistry.get_connector(task_template.type, task_template.task_type_version)

        resource_meta = local_connector_loop.run_until_complete(
            self._create(task_template=task_template, output_prefix=output_prefix, inputs=kwargs)
        )
        resource = local_connector_loop.run_until_complete(self._get(resource_meta=resource_meta))

        if resource.phase != TaskExecution.SUCCEEDED:
            raise FlyteUserException(f"Failed to run the task {self.name} with error: {resource.message}")

        # Read the literals from a remote file if the connector doesn't return the output literals.
        if task_template.interface.outputs and resource.outputs is None:
            local_outputs_file = ctx.file_access.get_random_local_path()
            ctx.file_access.get_data(f"{output_prefix}/outputs.pb", local_outputs_file)
            output_proto = utils.load_proto_from_file(literals_pb2.LiteralMap, local_outputs_file)
            return LiteralMap.from_flyte_idl(output_proto)

        if resource.outputs and not isinstance(resource.outputs, LiteralMap):
            return TypeEngine.dict_to_literal_map(ctx, resource.outputs)  # type: ignore

        return resource.outputs

    async def _create(
        self: T, task_template: TaskTemplate, output_prefix: str, inputs: Dict[str, Any] = None
    ) -> ResourceMeta:
        ctx = FlyteContext.current_context()
        if isinstance(self, PythonFunctionTask):
            es = ctx.new_execution_state().with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
            cb = ctx.new_builder().with_execution_state(es)

            with FlyteContextManager.with_context(cb) as ctx:
                # Write the inputs to a remote file, so that the remote task can read the inputs from this file.
                literal_map = await TypeEngine._dict_to_literal_map(ctx, inputs or {}, self.get_input_types())
                path = ctx.file_access.get_random_local_path()
                utils.write_proto_to_file(literal_map.to_flyte_idl(), path)
                await ctx.file_access.async_put_data(path, f"{output_prefix}/inputs.pb")
                task_template = render_task_template(task_template, output_prefix)
        else:
            literal_map = TypeEngine.dict_to_literal_map(ctx, inputs or {}, self.get_input_types())

        resource_meta = await mirror_async_methods(
            self._connector.create,
            task_template=task_template,
            inputs=literal_map,
            output_prefix=output_prefix,
        )

        FlyteContextManager.add_signal_handler(partial(self.connector_signal_handler, resource_meta))
        self.resource_meta = resource_meta
        return resource_meta

    async def _get(self: T, resource_meta: ResourceMeta) -> Resource:
        phase = TaskExecution.RUNNING

        progress = Progress(transient=True)
        set_flytekit_log_properties(RichHandler(log_time_format="%H:%M:%S.%f"), None, None)
        task = progress.add_task(f"[cyan]Running Task {self.name}...", total=None)
        task_phase = progress.add_task("[cyan]Task phase: RUNNING, Phase message: ", total=None, visible=False)
        task_log_links = progress.add_task("[cyan]Log Links: ", total=None, visible=False)
        with progress:
            while not is_terminal_phase(phase):
                progress.start_task(task)
                time.sleep(1)
                resource = await mirror_async_methods(self._connector.get, resource_meta=resource_meta)
                if self._clean_up_task:
                    sys.exit(1)

                phase = resource.phase
                progress.update(
                    task_phase,
                    description=f"[cyan]Task phase: {TaskExecution.Phase.Name(phase)}, Phase message: {resource.message}",
                    visible=True,
                )
                if resource.log_links:
                    log_links = ""
                    for link in resource.log_links:
                        log_links += f"{link.name}: {link.uri}\n"
                    if log_links:
                        progress.update(task_log_links, description=f"[cyan]{log_links}", visible=True)

        return resource

    def connector_signal_handler(self, resource_meta: ResourceMeta, signum: int, frame: FrameType) -> Any:
        if inspect.iscoroutinefunction(self._connector.delete):
            # Use asyncio.run to run the async function in the main thread since the loop manager is killed when the
            # signal is received.
            asyncio.run(self._connector.delete(resource_meta=resource_meta))
        else:
            self._connector.delete(resource_meta)
        self._clean_up_task = True
