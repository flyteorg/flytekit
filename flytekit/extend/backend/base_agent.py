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

from flyteidl.admin.agent_pb2 import (
    Agent,
)
from flyteidl.admin.agent_pb2 import TaskType as _TaskType
from flyteidl.core import literals_pb2
from flyteidl.core.execution_pb2 import TaskExecution, TaskLog
from rich.progress import Progress

from flytekit import FlyteContext, PythonFunctionTask, logger
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core import utils
from flytekit.core.base_task import PythonTask
from flytekit.core.type_engine import TypeEngine, dataclass_from_dict
from flytekit.exceptions.system import FlyteAgentNotFound
from flytekit.exceptions.user import FlyteUserException
from flytekit.extend.backend.utils import is_terminal_phase, mirror_async_methods, render_task_template
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class TaskType:
    def __init__(self, name: str, version: int = 0):
        self._name = name
        self._version = version

    def __hash__(self):
        return hash((self._name, self._version))

    def __eq__(self, other: "TaskType"):
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


T = typing.TypeVar("T", bound=ResourceMeta)


class AgentBase(ABC):
    name = "Base Agent"

    def __init__(self, task_type_name: str, task_type_version: int = 0, **kwargs):
        self._task_type = TaskType(name=task_type_name, version=task_type_version)

    @property
    def task_type(self) -> TaskType:
        """
        task type that the agent supports
        """
        return self._task_type


class SyncAgentBase(AgentBase):
    """
    This is the base class for all sync agents. It defines the interface that all agents must implement.
    The agent service is responsible for invoking agents.
    Propeller sends a request to agent service, and gets a response in the same call.

    All the agents should be registered in the AgentRegistry. Agent Service
    will look up the agent based on the task type. Every task type can only have one agent.
    """

    @abstractmethod
    def do(self, task_template: TaskTemplate, inputs: Optional[LiteralMap], **kwargs) -> Resource:
        pass


class AsyncAgentBase(AgentBase, typing.Generic[T]):
    """
    This is the base class for all async agents. It defines the interface that all agents must implement.
    The agent service is responsible for invoking agents. The propeller will communicate with the agent service
    to create tasks, get the status of tasks, and delete tasks.

    All the agents should be registered in the AgentRegistry. Agent Service
    will look up the agent based on the task type. Every task type can only have one agent.
    """

    name = "Base Async Agent"

    @abstractmethod
    def create(self, task_template: TaskTemplate, inputs: Optional[LiteralMap], **kwargs) -> T:
        """
        Return a resource meta that can be used to get the status of the task.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, resource_meta: T, **kwargs) -> Resource:
        """
        Return the status of the task, and return the outputs in some cases. For example, bigquery job
        can't write the structured dataset to the output location, so it returns the output literals to the propeller,
        and the propeller will write the structured dataset to the blob store.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, resource_meta: T, **kwargs):
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

    _REGISTRY: Dict[TaskType, Union[AsyncAgentBase, SyncAgentBase]] = {}
    METADATA: Dict[str, Agent] = {}

    @staticmethod
    def register(agent: Union[AsyncAgentBase, SyncAgentBase], override: bool = False):
        if agent.task_type in AgentRegistry._REGISTRY and override is False:
            raise ValueError(f"Duplicate agent for task type: {agent.task_type}")
        AgentRegistry._REGISTRY[agent.task_type] = agent

        task_type = _TaskType(name=agent.task_type.name, version=agent.task_type.version)

        if agent.name in AgentRegistry.METADATA:
            agent_metadata = AgentRegistry.METADATA[agent.name]
            agent_metadata.supported_task_types.append(task_type)
        else:
            agent_metadata = Agent(name=agent.name, supported_task_types=[task_type], is_sync=isinstance(agent, SyncAgentBase))
            AgentRegistry.METADATA[agent.name] = agent_metadata

        logger.info(f"Registering {agent.name} agent for task type: {agent.task_type}")

    @staticmethod
    def get_agent(task_type_name: str, task_type_version: int = 0) -> Union[SyncAgentBase, AsyncAgentBase]:
        task_type = TaskType(name=task_type_name, version=task_type_version)
        if task_type not in AgentRegistry._REGISTRY:
            raise FlyteAgentNotFound(f"Cannot find agent for task type: {task_type}.")
        return AgentRegistry._REGISTRY[task_type]

    @staticmethod
    def list_agents() -> List[Agent]:
        return list(AgentRegistry.METADATA.values())

    @staticmethod
    def get_agent_metadata(name: str) -> Agent:
        if name not in AgentRegistry.METADATA:
            raise FlyteAgentNotFound(f"Cannot find agent for name: {name}.")
        return AgentRegistry.METADATA[name]


class SyncAgentExecutorMixin:
    """
    TODO: Add documentation
    """

    T = typing.TypeVar("T", "SyncAgentExecutorMixin", PythonTask)

    def execute(self: T, **kwargs) -> LiteralMap:
        from flytekit.tools.translator import get_serializable

        ctx = FlyteContext.current_context()
        ss = ctx.serialization_settings or SerializationSettings(ImageConfig())
        task_template = get_serializable(OrderedDict(), ss, self).template

        agent = AgentRegistry.get_agent(task_template.type, task_template.task_type_version)

        resource = asyncio.run(self._do(agent, task_template, kwargs))
        if resource.outputs and not isinstance(resource.outputs, LiteralMap):
            return TypeEngine.dict_to_literal_map(ctx, resource.outputs)
        return resource.outputs

    async def _do(self: T, agent: SyncAgentBase, template: TaskTemplate, inputs: Dict[str, Any] = None) -> Resource:
        ctx = FlyteContext.current_context()
        literal_map = TypeEngine.dict_to_literal_map(ctx, inputs or {}, self.get_input_types())
        return await mirror_async_methods(agent.do, task_template=template, inputs=literal_map)


class AsyncAgentExecutorMixin:
    """
    This mixin class is used to run the agent task locally, and it's only used for local execution.
    Task should inherit from this class if the task can be run in the agent.
    It can handle asynchronous tasks and synchronous tasks.
    Asynchronous tasks are tasks that take a long time to complete, such as running a query.
    Synchronous tasks run quickly and can return their results instantly. Sending a prompt to ChatGPT and getting a response, or retrieving some metadata from a backend system.
    """

    T = typing.TypeVar("T", "AsyncAgentExecutorMixin", PythonTask)

    _clean_up_task: coroutine = None
    _agent: AsyncAgentBase = None

    def execute(self: T, **kwargs) -> LiteralMap:
        ctx = FlyteContext.current_context()
        ss = ctx.serialization_settings or SerializationSettings(ImageConfig())
        output_prefix = ctx.file_access.get_random_remote_directory()

        from flytekit.tools.translator import get_serializable

        task_template = get_serializable(OrderedDict(), ss, self).template
        self._agent = AgentRegistry.get_agent(task_template.type, task_template.task_type_version)

        resource_mata = asyncio.run(self._create(task_template, output_prefix, kwargs))
        resource = asyncio.run(self._get(resource_meta=resource_mata))

        if resource.phase != TaskExecution.SUCCEEDED:
            raise FlyteUserException(f"Failed to run the task {self.name} with error: {resource.message}")

        # Read the literals from a remote file if the agent doesn't return the output literals.
        if task_template.interface.outputs and resource.outputs and len(resource.outputs.literals) == 0:
            local_outputs_file = ctx.file_access.get_random_local_path()
            ctx.file_access.get_data(f"{output_prefix}/outputs.pb", local_outputs_file)
            output_proto = utils.load_proto_from_file(literals_pb2.LiteralMap, local_outputs_file)
            return LiteralMap.from_flyte_idl(output_proto)

        if resource.outputs and not isinstance(resource.outputs, LiteralMap):
            return TypeEngine.dict_to_literal_map(ctx, resource.outputs)

        return resource.outputs

    async def _create(
        self: T, task_template: TaskTemplate, output_prefix: str, inputs: Dict[str, Any] = None
    ) -> ResourceMeta:
        ctx = FlyteContext.current_context()

        literal_map = TypeEngine.dict_to_literal_map(ctx, inputs or {}, self.get_input_types())
        if isinstance(self, PythonFunctionTask):
            # Write the inputs to a remote file, so that the remote task can read the inputs from this file.
            path = ctx.file_access.get_random_local_path()
            utils.write_proto_to_file(literal_map.to_flyte_idl(), path)
            ctx.file_access.put_data(path, f"{output_prefix}/inputs.pb")
            task_template = render_task_template(task_template, output_prefix)

        resource_meta = await mirror_async_methods(
            self._agent.create,
            task_template=task_template,
            inputs=literal_map,
        )

        signal.signal(signal.SIGINT, partial(self.signal_handler, resource_meta))  # type: ignore
        return resource_meta

    async def _get(self: T, resource_meta: ResourceMeta) -> Resource:
        phase = TaskExecution.RUNNING

        progress = Progress(transient=True)
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
