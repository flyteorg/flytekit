import asyncio
import signal
import sys
import time
import typing
from abc import ABC
from collections import OrderedDict
from functools import partial
from types import FrameType

import grpc
from flyteidl.admin.agent_pb2 import (
    PERMANENT_FAILURE,
    RETRYABLE_FAILURE,
    RUNNING,
    SUCCEEDED,
    CreateTaskResponse,
    DeleteTaskResponse,
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
    if state in ["failed", "timedout", "canceled"]:
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


class AsyncAgentExecutorMixin:
    """
    This mixin class is used to run the agent task locally, and it's only used for local execution.
    Task should inherit from this class if the task can be run in the agent.
    """

    _is_canceled = None
    _agent = None
    _entity = None

    def execute(self, **kwargs) -> typing.Any:
        from flytekit.tools.translator import get_serializable

        self._entity = typing.cast(PythonTask, self)
        task_template = get_serializable(OrderedDict(), SerializationSettings(ImageConfig()), self._entity).template
        self._agent = AgentRegistry.get_agent(task_template.type)

        res = asyncio.run(self._create(task_template, kwargs))
        res = asyncio.run(self._get(resource_meta=res.resource_meta))

        if res.resource.state != SUCCEEDED:
            raise FlyteUserException(f"Failed to run the task {self._entity.name}")

        return LiteralMap.from_flyte_idl(res.resource.outputs)

    async def _create(
        self, task_template: TaskTemplate, inputs: typing.Dict[str, typing.Any] = None
    ) -> CreateTaskResponse:
        ctx = FlyteContext.current_context()
        grpc_ctx = _get_grpc_context()

        # Convert python inputs to literals
        literals = {}
        for k, v in inputs.items():
            literals[k] = TypeEngine.to_literal(ctx, v, type(v), self._entity.interface.inputs[k].type)
        inputs = LiteralMap(literals) if literals else None
        output_prefix = ctx.file_access.get_random_local_directory()

        if self._agent.asynchronous:
            res = await self._agent.async_create(grpc_ctx, output_prefix, task_template, inputs)
        else:
            res = self._agent.create(grpc_ctx, output_prefix, task_template, inputs)

        signal.signal(signal.SIGINT, partial(self.signal_handler, res.resource_meta))  # type: ignore
        return res

    async def _get(self, resource_meta: bytes) -> GetTaskResponse:
        state = RUNNING
        grpc_ctx = _get_grpc_context()

        while not is_terminal_state(state):
            time.sleep(1)
            if self._agent.asynchronous:
                res = await self._agent.async_get(grpc_ctx, resource_meta)
                if self._is_canceled:
                    await self._is_canceled
                    sys.exit(1)
            else:
                res = self._agent.get(grpc_ctx, resource_meta)
            state = res.resource.state
            logger.info(f"Task state: {state}")
        return res

    def signal_handler(self, resource_meta: bytes, signum: int, frame: FrameType) -> typing.Any:
        grpc_ctx = _get_grpc_context()
        if self._agent.asynchronous:
            if self._is_canceled is None:
                self._is_canceled = asyncio.create_task(self._agent.async_delete(grpc_ctx, resource_meta))
        else:
            self._agent.delete(grpc_ctx, resource_meta)
            sys.exit(1)


def _get_grpc_context():
    from unittest.mock import MagicMock

    grpc_ctx = MagicMock(spec=grpc.ServicerContext)
    return grpc_ctx
