import asyncio
import signal
import sys
import time
import typing
from abc import ABC
from collections import OrderedDict
from functools import partial
from types import FrameType, coroutine

import grpc
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    RETRYABLE_FAILURE,
    RUNNING,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
    DoTaskResponse,
    GetTaskResponse,
    State,
)
from flyteidl.core.tasks_pb2 import TaskTemplate

import flytekit
from flytekit import FlyteContext, logger
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.type_engine import TypeEngine
from flytekit.exceptions.system import FlyteAgentNotFound
from flytekit.exceptions.user import FlyteUserException
from flytekit.models.literals import LiteralMap

SYNC_PLUGIN = "sync_plugin"  # Indicates that the sync plugin in FlytePropeller should be used to run this task
ASYNC_PLUGIN = "async_plugin"  # Indicates that the async plugin in FlytePropeller should be used to run this task


class AgentBase(ABC):
    """
    This is the base class for all agents. It defines the interface that all agents must implement.
    The agent service will be run either locally or in a pod, and will be responsible for
    invoking agents. The propeller will communicate with the agent service
    to create tasks, get the status of tasks, and delete tasks.

    All the agents should be registered in the AgentRegistry. Agent Service
    will look up the agent based on the task type. Every task type can only have one agent.
    """

    def __init__(self, task_type: str, asynchronous=True):
        self._task_type = task_type
        self._asynchronous = asynchronous

    @property
    def asynchronous(self) -> bool:
        """
        asynchronous is a flag to indicate whether the agent is asynchronous or not.
        """
        return self._asynchronous

    @property
    def task_type(self) -> str:
        """
        task_type is the name of the task type that this agent supports.
        """
        return self._task_type

    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        """
        Return a Unique ID for the task that was created. It should return error code if the task creation failed.
        """
        raise NotImplementedError

    def get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        raise NotImplementedError

    def delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        """
        Delete the task. This call should be idempotent.
        """
        raise NotImplementedError

    def do(
        self,
        context: grpc.ServicerContext,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> DoTaskResponse:
        """
        Return the result of executing a task. It should return error code if the task execution failed.
        """
        raise NotImplementedError

    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        """
        Return a Unique ID for the task that was created. It should return error code if the task creation failed.
        """
        raise NotImplementedError

    async def async_get(self, context: grpc.ServicerContext, resource_meta: bytes) -> GetTaskResponse:
        """
        Return the status of the task, and return the outputs in some cases. For example, bigquery job
        can't write the structured dataset to the output location, so it returns the output literals to the propeller,
        and the propeller will write the structured dataset to the blob store.
        """
        raise NotImplementedError

    async def async_delete(self, context: grpc.ServicerContext, resource_meta: bytes) -> DeleteTaskResponse:
        """
        Delete the task. This call should be idempotent.
        """
        raise NotImplementedError

    async def async_do(
        self,
        context: grpc.ServicerContext,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> DoTaskResponse:
        """
        Return the result of executing a task. It should return error code if the task execution failed.
        """
        raise NotImplementedError


class AgentRegistry(object):
    """
    This is the registry for all agents. The agent service will look up the agent
    based on the task type.
    """

    _REGISTRY: typing.Dict[str, AgentBase] = {}

    @staticmethod
    def register(agent: AgentBase):
        if agent.task_type in AgentRegistry._REGISTRY:
            raise ValueError(f"Duplicate agent for task type {agent.task_type}")
        AgentRegistry._REGISTRY[agent.task_type] = agent
        logger.info(f"Registering an agent for task type {agent.task_type}")

    @staticmethod
    def get_agent(task_type: str) -> typing.Optional[AgentBase]:
        if task_type not in AgentRegistry._REGISTRY:
            raise FlyteAgentNotFound(f"Cannot find agent for task type: {task_type}.")
        return AgentRegistry._REGISTRY[task_type]


def convert_to_flyte_state(state: str) -> State:
    """
    Convert the state from the agent to the state in flyte.
    """
    state = state.lower()
    # timedout is the state of Databricks job. https://docs.databricks.com/en/workflows/jobs/jobs-2.0-api.html#runresultstate
    if state in ["failed", "timeout", "timedout", "canceled"]:
        return RETRYABLE_FAILURE
    elif state in ["done", "succeeded", "success"]:
        return SUCCEEDED
    elif state in ["running"]:
        return RUNNING
    raise ValueError(f"Unrecognized state: {state}")


def is_terminal_state(state: State) -> bool:
    """
    Return true if the state is terminal.
    """
    return state in [SUCCEEDED, RETRYABLE_FAILURE, PERMANENT_FAILURE]


def get_agent_secret(secret_key: str) -> str:
    return flytekit.current_context().secrets.get(secret_key)


def _get_grpc_context() -> grpc.ServicerContext:
    from unittest.mock import MagicMock

    grpc_ctx = MagicMock(spec=grpc.ServicerContext)
    return grpc_ctx


class AsyncAgentExecutorMixin:
    """
    This mixin class is used to run the agent task locally, and it's only used for local execution.
    Task should inherit from this class if the task can be run in the agent.
    """

    _clean_up_task: coroutine = None
    _agent: AgentBase = None
    _entity: PythonTask = None
    _ctx: FlyteContext = FlyteContext.current_context()
    _grpc_ctx: grpc.ServicerContext = _get_grpc_context()

    def execute(self, **kwargs) -> typing.Any:
        from flytekit.extend.backend.task_executor import TaskExecutor  # This is for circular import avoidance.
        from flytekit.tools.translator import get_serializable

        self._entity = typing.cast(PythonTask, self)
        task_template = get_serializable(OrderedDict(), SerializationSettings(ImageConfig()), self._entity).template
        self._agent = AgentRegistry.get_agent(task_template.type)

        if isinstance(self._agent, TaskExecutor):
            res = asyncio.run(self._do(task_template, kwargs))
        else:
            res = asyncio.run(self._create(task_template, kwargs))
            res = asyncio.run(self._get(resource_meta=res.resource_meta))

        if res.resource.state != SUCCEEDED:
            raise FlyteUserException(f"Failed to run the task {self._entity.name}")

        return LiteralMap.from_flyte_idl(res.resource.outputs)

    async def _create(
        self, task_template: TaskTemplate, inputs: typing.Dict[str, typing.Any] = None
    ) -> CreateTaskResponse:
        inputs = self.get_input_literal_map(inputs)
        output_prefix = self._ctx.file_access.get_random_local_directory()

        if self._agent.asynchronous:
            res = await self._agent.async_create(self._grpc_ctx, output_prefix, task_template, inputs)
        else:
            res = self._agent.create(self._grpc_ctx, output_prefix, task_template, inputs)

        signal.signal(signal.SIGINT, partial(self.signal_handler, res.resource_meta))  # type: ignore
        return res

    async def _get(self, resource_meta: bytes) -> GetTaskResponse:
        state = RUNNING
        while not is_terminal_state(state):
            time.sleep(1)
            if self._agent.asynchronous:
                res = await self._agent.async_get(self._grpc_ctx, resource_meta)
                if self._clean_up_task:
                    await self._clean_up_task
                    sys.exit(1)
            else:
                res = self._agent.get(self._grpc_ctx, resource_meta)
            state = res.resource.state
            logger.info(f"Task state: {state}")
        return res

    async def _do(self, task_template: TaskTemplate, inputs: typing.Dict[str, typing.Any] = None):
        inputs = self.get_input_literal_map(inputs)
        if self._agent.asynchronous:
            res = await self._agent.async_do(self._grpc_ctx, task_template, inputs)
        else:
            res = self._agent.do(self._grpc_ctx, task_template, inputs)
        return res

    def signal_handler(self, resource_meta: bytes, signum: int, frame: FrameType) -> typing.Any:
        if self._agent.asynchronous:
            if self._clean_up_task is None:
                self._clean_up_task = asyncio.create_task(self._agent.async_delete(self._grpc_ctx, resource_meta))
        else:
            self._agent.delete(self._grpc_ctx, resource_meta)
            sys.exit(1)

    def get_input_literal_map(self, inputs: typing.Dict[str, typing.Any] = None) -> typing.Optional[LiteralMap]:
        if inputs is None:
            return None
        # Convert python inputs to literals
        literals = {}
        for k, v in inputs.items():
            literals[k] = TypeEngine.to_literal(self._ctx, v, type(v), self._entity.interface.inputs[k].type)
        return LiteralMap(literals) if literals else None
