import asyncio
import json
import signal
import sys
import time
import typing
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import asdict, dataclass
from functools import partial
from types import FrameType, coroutine
from typing import Any, Dict, List, Optional, Union

from flyteidl.admin.agent_pb2 import Agent
from flyteidl.admin.agent_pb2 import TaskCategory as _TaskCategory
from flyteidl.core import literals_pb2
from flyteidl.core.execution_pb2 import TaskExecution, TaskLog
from rich.logging import RichHandler
from rich.progress import Progress

from flytekit import FlyteContext, PythonFunctionTask
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core import utils
from flytekit.core.base_task import PythonTask
from flytekit.core.context_manager import ExecutionState, FlyteContextManager
from flytekit.core.type_engine import TypeEngine, dataclass_from_dict
from flytekit.exceptions.system import FlyteAgentNotFound
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend.backend.utils import is_terminal_phase, mirror_async_methods, render_task_template
from flytekit.loggers import set_flytekit_log_properties
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskExecutionMetadata, TaskTemplate


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

    Args:
        phase: The phase of the job.
        message: The return message from the job.
        log_links: The log links of the job. For example, the link to the BigQuery Console.
        outputs: The outputs of the job. If return python native types, the agent will convert them to flyte literals.
    """

    phase: TaskExecution.Phase
    message: Optional[str] = None
    log_links: Optional[List[TaskLog]] = None
    outputs: Optional[Union[LiteralMap, typing.Dict[str, Any]]] = None


class AgentBase(ABC):
    name = "Base Agent"

    def __init__(self, task_type_name: str, task_type_version: int = 0, **kwargs):
        self._task_category = TaskCategory(name=task_type_name, version=task_type_version)

    @property
    def task_category(self) -> TaskCategory:
        """
        task category that the agent supports
        """
        return self._task_category


class SyncAgentBase(AgentBase):
    """
    This is the base class for all sync agents. It defines the interface that all agents must implement.
    The agent service is responsible for invoking agents.
    Propeller sends a request to agent service, and gets a response in the same call.

    All the agents should be registered in the AgentRegistry. Agent Service
    will look up the agent based on the task type. Every task type can only have one agent.
    """

    name = "Base Sync Agent"

    @abstractmethod
    def do(
        self, task_template: TaskTemplate, output_prefix: str, inputs: Optional[LiteralMap] = None, **kwargs
    ) -> Resource:
        """
        This is the method that the agent will run.
        """
        raise NotImplementedError


class AsyncAgentBase(AgentBase):
    """
    This is the base class for all async agents. It defines the interface that all agents must implement.
    The agent service is responsible for invoking agents. The propeller will communicate with the agent service
    to create tasks, get the status of tasks, and delete tasks.

    All the agents should be registered in the AgentRegistry. Agent Service
    will look up the agent based on the task type. Every task type can only have one agent.
    """

    name = "Base Async Agent"

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


class AgentRegistry(object):
    """
    This is the registry for all agents.
    The agent service will look up the agent registry based on the task type.
    The agent metadata service will look up the agent metadata based on the agent name.
    """

    _REGISTRY: Dict[TaskCategory, Union[AsyncAgentBase, SyncAgentBase]] = {}
    _METADATA: Dict[str, Agent] = {}

    @staticmethod
    def register(agent: Union[AsyncAgentBase, SyncAgentBase], override: bool = False):
        if agent.task_category in AgentRegistry._REGISTRY and override is False:
            raise ValueError(f"Duplicate agent for task type: {agent.task_category}")
        AgentRegistry._REGISTRY[agent.task_category] = agent

        task_category = _TaskCategory(name=agent.task_category.name, version=agent.task_category.version)

        if agent.name in AgentRegistry._METADATA:
            agent_metadata = AgentRegistry.get_agent_metadata(agent.name)
            agent_metadata.supported_task_categories.append(task_category)
            agent_metadata.supported_task_types.append(task_category.name)
        else:
            agent_metadata = Agent(
                name=agent.name,
                supported_task_types=[task_category.name],
                supported_task_categories=[task_category],
                is_sync=isinstance(agent, SyncAgentBase),
            )
            AgentRegistry._METADATA[agent.name] = agent_metadata

    @staticmethod
    def get_agent(task_type_name: str, task_type_version: int = 0) -> Union[SyncAgentBase, AsyncAgentBase]:
        task_category = TaskCategory(name=task_type_name, version=task_type_version)
        if task_category not in AgentRegistry._REGISTRY:
            raise FlyteAgentNotFound(f"Cannot find agent for task category: {task_category}.")
        return AgentRegistry._REGISTRY[task_category]

    @staticmethod
    def list_agents() -> List[Agent]:
        return list(AgentRegistry._METADATA.values())

    @staticmethod
    def get_agent_metadata(name: str) -> Agent:
        if name not in AgentRegistry._METADATA:
            raise FlyteAgentNotFound(f"Cannot find agent for name: {name}.")
        return AgentRegistry._METADATA[name]


class SyncAgentExecutorMixin:
    """
    This mixin class is used to run the sync task locally, and it's only used for local execution.
    Task should inherit from this class if the task can be run in the agent.

    Synchronous tasks run quickly and can return their results instantly.
    Sending a prompt to ChatGPT and getting a response, or retrieving some metadata from a backend system.
    """

    def execute(self: PythonTask, **kwargs) -> LiteralMap:
        from flytekit.tools.translator import get_serializable

        ctx = FlyteContext.current_context()
        ss = ctx.serialization_settings or SerializationSettings(ImageConfig())
        task_template = get_serializable(OrderedDict(), ss, self).template
        output_prefix = ctx.file_access.get_random_remote_directory()

        agent = AgentRegistry.get_agent(task_template.type, task_template.task_type_version)

        resource = asyncio.run(
            self._do(agent=agent, template=task_template, output_prefix=output_prefix, inputs=kwargs)
        )
        if resource.phase != TaskExecution.SUCCEEDED:
            raise FlyteUserException(f"Failed to run the task {self.name} with error: {resource.message}")

        if resource.outputs and not isinstance(resource.outputs, LiteralMap):
            return TypeEngine.dict_to_literal_map(ctx, resource.outputs)
        return resource.outputs

    async def _do(
        self: PythonTask,
        agent: SyncAgentBase,
        template: TaskTemplate,
        output_prefix: str,
        inputs: Dict[str, Any] = None,
    ) -> Resource:
        try:
            ctx = FlyteContext.current_context()
            literal_map = TypeEngine.dict_to_literal_map(ctx, inputs or {}, self.get_input_types())
            return await mirror_async_methods(
                agent.do, task_template=template, inputs=literal_map, output_prefix=output_prefix
            )
        except Exception as e:
            raise FlyteUserException(f"Failed to run the task {self.name} with error: {e}") from None


class AsyncAgentExecutorMixin:
    """
    This mixin class is used to run the async task locally, and it's only used for local execution.
    Task should inherit from this class if the task can be run in the agent.

    Asynchronous tasks are tasks that take a long time to complete, such as running a query.
    """

    _clean_up_task: coroutine = None
    _agent: AsyncAgentBase = None

    def execute(self: PythonTask, **kwargs) -> LiteralMap:
        ctx = FlyteContext.current_context()
        ss = ctx.serialization_settings or SerializationSettings(ImageConfig())
        output_prefix = ctx.file_access.get_random_remote_directory()

        from flytekit.tools.translator import get_serializable

        task_template = get_serializable(OrderedDict(), ss, self).template
        self._agent = AgentRegistry.get_agent(task_template.type, task_template.task_type_version)

        resource_mata = asyncio.run(
            self._create(task_template=task_template, output_prefix=output_prefix, inputs=kwargs)
        )
        resource = asyncio.run(self._get(resource_meta=resource_mata))

        if resource.phase != TaskExecution.SUCCEEDED:
            raise FlyteUserException(f"Failed to run the task {self.name} with error: {resource.message}")

        # Read the literals from a remote file if the agent doesn't return the output literals.
        if task_template.interface.outputs and resource.outputs is None:
            local_outputs_file = ctx.file_access.get_random_local_path()
            ctx.file_access.get_data(f"{output_prefix}/outputs.pb", local_outputs_file)
            output_proto = utils.load_proto_from_file(literals_pb2.LiteralMap, local_outputs_file)
            return LiteralMap.from_flyte_idl(output_proto)

        if resource.outputs and not isinstance(resource.outputs, LiteralMap):
            return TypeEngine.dict_to_literal_map(ctx, resource.outputs)

        return resource.outputs

    async def _create(
        self: PythonTask, task_template: TaskTemplate, output_prefix: str, inputs: Dict[str, Any] = None
    ) -> ResourceMeta:
        ctx = FlyteContext.current_context()
        if isinstance(self, PythonFunctionTask):
            es = ctx.new_execution_state().with_params(mode=ExecutionState.Mode.TASK_EXECUTION)
            cb = ctx.new_builder().with_execution_state(es)

            with FlyteContextManager.with_context(cb) as ctx:
                # Write the inputs to a remote file, so that the remote task can read the inputs from this file.
                literal_map = TypeEngine.dict_to_literal_map(ctx, inputs or {}, self.get_input_types())
                path = ctx.file_access.get_random_local_path()
                utils.write_proto_to_file(literal_map.to_flyte_idl(), path)
                ctx.file_access.put_data(path, f"{output_prefix}/inputs.pb")
                task_template = render_task_template(task_template, output_prefix)
        else:
            literal_map = TypeEngine.dict_to_literal_map(ctx, inputs or {}, self.get_input_types())

        resource_meta = await mirror_async_methods(
            self._agent.create,
            task_template=task_template,
            inputs=literal_map,
            output_prefix=output_prefix,
        )

        signal.signal(signal.SIGINT, partial(self.signal_handler, resource_meta))  # type: ignore
        return resource_meta

    async def _get(self: PythonTask, resource_meta: ResourceMeta) -> Resource:
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
                resource = await mirror_async_methods(self._agent.get, resource_meta=resource_meta)
                if self._clean_up_task:
                    await self._clean_up_task
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

    def signal_handler(self, resource_meta: ResourceMeta, signum: int, frame: FrameType) -> Any:
        if self._clean_up_task is None:
            co = mirror_async_methods(self._agent.delete, resource_meta=resource_meta)
            self._clean_up_task = asyncio.create_task(co)
