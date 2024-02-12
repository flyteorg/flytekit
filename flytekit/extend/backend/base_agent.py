import asyncio
import inspect
import signal
import sys
import time
import typing
from abc import ABC, abstractmethod
from collections import OrderedDict
from functools import partial
from types import FrameType, coroutine
from typing import Any, Callable, Coroutine, Dict, Iterator, List, Optional, Union, cast

from flyteidl.admin.agent_pb2 import (
    Agent,
    CreateTaskResponse,
    DeleteTaskResponse,
    ExecuteTaskSyncResponse,
    GetTaskResponse,
    TaskType,
)
from flyteidl.core import literals_pb2
from flyteidl.core.execution_pb2 import TaskExecution
from flyteidl.core.tasks_pb2 import TaskTemplate
from rich.progress import Progress

import flytekit
from flytekit import FlyteContext, PythonFunctionTask, logger
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core import utils
from flytekit.core.base_task import PythonTask
from flytekit.core.type_engine import TypeEngine
from flytekit.exceptions.system import FlyteAgentNotFound
from flytekit.exceptions.user import FlyteUserException
from flytekit.models.literals import LiteralMap


class AgentBase(ABC):
    name = "Base Agent"

    def __init__(self, task_type_name: str, task_type_version: int = 0, **kwargs):
        self._task_type_name = task_type_name
        self._task_type_version = task_type_version

    @property
    def task_type_name(self) -> str:
        """
        task_type_name is the name of the task type that this agent supports.
        """
        return self._task_type_name

    @property
    def task_type_version(self) -> int:
        """
        task_type_version is the version of the task type that this agent supports.
        """
        return self._task_type_version


class SyncAgentBase(AgentBase):
    """
    This is the base class for all sync agents. It defines the interface that all agents must implement.
    The agent service is responsible for invoking agents.
    Propeller sends a request to agent service, and gets a response in the same call.

    All the agents should be registered in the AgentRegistry. Agent Service
    will look up the agent based on the task type. Every task type can only have one agent.
    """

    @abstractmethod
    def do(
        self,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[typing.Iterable[LiteralMap]] = None,
        **kwargs,
    ) -> Iterator[ExecuteTaskSyncResponse]:
        pass


class AsyncAgentBase(AgentBase):
    """
    This is the base class for all async agents. It defines the interface that all agents must implement.
    The agent service is responsible for invoking agents. The propeller will communicate with the agent service
    to create tasks, get the status of tasks, and delete tasks.

    All the agents should be registered in the AgentRegistry. Agent Service
    will look up the agent based on the task type. Every task type can only have one agent.
    """

    name = "Base Async Agent"

    @abstractmethod
    def create(
        self,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> CreateTaskResponse:
        """
        Return a Unique ID for the task that was created. It should return error code if the task creation failed.
        """
        raise NotImplementedError

    @abstractmethod
    def get(self, resource_meta: bytes) -> GetTaskResponse:
        """
        Return the status of the task, and return the outputs in some cases. For example, bigquery job
        can't write the structured dataset to the output location, so it returns the output literals to the propeller,
        and the propeller will write the structured dataset to the blob store.
        """
        raise NotImplementedError

    @abstractmethod
    def delete(self, resource_meta: bytes) -> DeleteTaskResponse:
        """
        Delete the task. This call should be idempotent.
        """
        raise NotImplementedError


class AgentRegistry(object):
    """
    This is the registry for all agents.
    The agent service will look up the agent registry based on the task type.
    The agent metadata service will look up the agent metadata based on the agent name.
    """

    _REGISTRY: Dict[str, Dict[int, Union[AsyncAgentBase, SyncAgentBase]]] = {}
    _METADATA: Dict[str, Agent] = {}

    @staticmethod
    def register(agent: Union[AsyncAgentBase, SyncAgentBase], override: bool = False):
        if (
            agent.task_type_name in AgentRegistry._REGISTRY
            and agent.task_type_version in AgentRegistry._REGISTRY[agent.task_type_name]
            and override is False
        ):
            raise ValueError(
                f"Duplicate agent for task type: {agent.task_type_name}, version: {agent.task_type_version}"
            )
        AgentRegistry._REGISTRY[agent.task_type_name] = {agent.task_type_version: agent}

        task_type = TaskType(name=agent.task_type_name, version=agent.task_type_version)

        if agent.name in AgentRegistry._METADATA:
            agent_metadata = AgentRegistry._METADATA[agent.name]
            agent_metadata.supported_task_types.append(task_type)
        else:
            agent_metadata = Agent(name=agent.name, supported_task_types=[task_type])
            AgentRegistry._METADATA[agent.name] = agent_metadata

        logger.info(
            f"Registering {agent.name} agent for task type: {agent.task_type_name}, version: {agent.task_type_version}"
        )

    @staticmethod
    def get_agent(task_type_name: str, task_type_version: int = 0) -> Union[SyncAgentBase, AsyncAgentBase]:
        if (
            task_type_name not in AgentRegistry._REGISTRY
            or task_type_version not in AgentRegistry._REGISTRY[task_type_name]
        ):
            raise FlyteAgentNotFound(f"Cannot find agent for task type: {task_type_name} version: {task_type_version}.")
        return AgentRegistry._REGISTRY[task_type_name][task_type_version]

    @staticmethod
    def list_agents() -> List[Agent]:
        return list(AgentRegistry._METADATA.values())

    @staticmethod
    def get_agent_metadata(name: str) -> Agent:
        if name not in AgentRegistry._METADATA:
            raise FlyteAgentNotFound(f"Cannot find agent for name: {name}.")
        return AgentRegistry._METADATA[name]


def mirror_async_methods(func: Callable, **kwargs) -> Coroutine:
    if inspect.iscoroutinefunction(func):
        return func(**kwargs)
    args = [v for _, v in kwargs.items()]
    return asyncio.get_running_loop().run_in_executor(None, func, *args)


def convert_to_flyte_phase(state: str) -> TaskExecution.Phase:
    """
    Convert the state from the agent to the phase in flyte.
    """
    state = state.lower()
    # timedout is the state of Databricks job. https://docs.databricks.com/en/workflows/jobs/jobs-2.0-api.html#runresultstate
    if state in ["failed", "timeout", "timedout", "canceled"]:
        return TaskExecution.FAILED
    elif state in ["done", "succeeded", "success"]:
        return TaskExecution.SUCCEEDED
    elif state in ["running"]:
        return TaskExecution.RUNNING
    raise ValueError(f"Unrecognized state: {state}")


def is_terminal_phase(phase: TaskExecution.Phase) -> bool:
    """
    Return true if the phase is terminal.
    """
    return phase in [TaskExecution.SUCCEEDED, TaskExecution.ABORTED, TaskExecution.FAILED]


def get_agent_secret(secret_key: str) -> str:
    return flytekit.current_context().secrets.get(secret_key)


class AsyncAgentExecutorMixin:
    """
    This mixin class is used to run the agent task locally, and it's only used for local execution.
    Task should inherit from this class if the task can be run in the agent.
    It can handle asynchronous tasks and synchronous tasks.
    Asynchronous tasks are tasks that take a long time to complete, such as running a query.
    Synchronous tasks run quickly and can return their results instantly. Sending a prompt to ChatGPT and getting a response, or retrieving some metadata from a backend system.
    """

    _clean_up_task: coroutine = None
    _agent: AsyncAgentBase = None
    _entity: PythonTask = None

    def execute(self, **kwargs) -> Any:
        ctx = FlyteContext.current_context()
        ss = ctx.serialization_settings or SerializationSettings(ImageConfig())
        output_prefix = ctx.file_access.get_random_remote_directory()

        from flytekit.tools.translator import get_serializable

        self._entity = cast(PythonTask, self)
        task_template = get_serializable(OrderedDict(), ss, self._entity).template
        self._agent = AgentRegistry.get_agent(task_template.type, task_template.task_type_version)

        res = asyncio.run(self._create(task_template, output_prefix, kwargs))
        res = asyncio.run(self._get(resource_meta=res.resource_meta))

        if res.resource.phase != TaskExecution.SUCCEEDED:
            raise FlyteUserException(f"Failed to run the task {self._entity.name}")

        # Read the literals from a remote file if the agent doesn't return the output literals.
        if task_template.interface.outputs and len(res.resource.outputs.literals) == 0:
            local_outputs_file = ctx.file_access.get_random_local_path()
            ctx.file_access.get_data(f"{output_prefix}/outputs.pb", local_outputs_file)
            output_proto = utils.load_proto_from_file(literals_pb2.LiteralMap, local_outputs_file)
            return LiteralMap.from_flyte_idl(output_proto)

        return LiteralMap.from_flyte_idl(res.resource.outputs)

    async def _create(
        self, task_template: TaskTemplate, output_prefix: str, inputs: Dict[str, Any] = None
    ) -> CreateTaskResponse:
        ctx = FlyteContext.current_context()

        # Convert python inputs to literals
        literals = inputs or {}
        for k, v in inputs.items():
            literals[k] = TypeEngine.to_literal(ctx, v, type(v), self._entity.interface.inputs[k].type)
        literal_map = LiteralMap(literals)

        if isinstance(self, PythonFunctionTask):
            # Write the inputs to a remote file, so that the remote task can read the inputs from this file.
            path = ctx.file_access.get_random_local_path()
            utils.write_proto_to_file(literal_map.to_flyte_idl(), path)
            ctx.file_access.put_data(path, f"{output_prefix}/inputs.pb")
            task_template = render_task_template(task_template, output_prefix)

        res = await mirror_async_methods(
            self._agent.create,
            output_prefix=output_prefix,
            task_template=task_template,
            inputs=literal_map,
        )

        signal.signal(signal.SIGINT, partial(self.signal_handler, res.resource_meta))  # type: ignore
        return res

    async def _get(self, resource_meta: bytes) -> GetTaskResponse:
        phase = TaskExecution.RUNNING

        progress = Progress(transient=True)
        task = progress.add_task(f"[cyan]Running Task {self._entity.name}...", total=None)
        task_phase = progress.add_task("[cyan]Task phase: RUNNING, Phase message: ", total=None, visible=False)
        task_log_links = progress.add_task("[cyan]Log Links: ", total=None, visible=False)
        with progress:
            while not is_terminal_phase(phase):
                progress.start_task(task)
                time.sleep(1)
                res = await mirror_async_methods(self._agent.get, resource_meta=resource_meta)
                if self._clean_up_task:
                    await self._clean_up_task
                    sys.exit(1)

                phase = res.resource.phase
                progress.update(
                    task_phase,
                    description=f"[cyan]Task phase: {TaskExecution.Phase.Name(phase)}, Phase message: {res.resource.message}",
                    visible=True,
                )
                log_links = ""
                for link in res.log_links:
                    log_links += f"{link.name}: {link.uri}\n"
                if log_links:
                    progress.update(task_log_links, description=f"[cyan]{log_links}", visible=True)

        return res

    def signal_handler(self, resource_meta: bytes, signum: int, frame: FrameType) -> Any:
        if self._clean_up_task is None:
            co = mirror_async_methods(self._agent.delete, resource_meta=resource_meta)
            self._clean_up_task = asyncio.create_task(co)


def render_task_template(tt: TaskTemplate, file_prefix: str) -> TaskTemplate:
    args = tt.container.args
    for i in range(len(args)):
        tt.container.args[i] = args[i].replace("{{.input}}", f"{file_prefix}/inputs.pb")
        tt.container.args[i] = args[i].replace("{{.outputPrefix}}", f"{file_prefix}")
        tt.container.args[i] = args[i].replace("{{.rawOutputDataPrefix}}", f"{file_prefix}/raw_output")
        tt.container.args[i] = args[i].replace("{{.checkpointOutputPrefix}}", f"{file_prefix}/checkpoint_output")
        tt.container.args[i] = args[i].replace("{{.prevCheckpointPrefix}}", f"{file_prefix}/prev_checkpoint")
    return tt
