import asyncio
import signal
import sys
import time
import typing
from abc import ABC
from asyncio import AbstractEventLoop
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
from rich.progress import Progress

from flytekit import FlyteContext, logger
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.type_engine import TypeEngine
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
    def get_agent(context: grpc.ServicerContext, task_type: str) -> typing.Optional[AgentBase]:
        if task_type not in AgentRegistry._REGISTRY:
            logger.error(f"Cannot find agent for task type [{task_type}]")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Cannot find the agent for task type [{task_type}]")
            return None
        return AgentRegistry._REGISTRY[task_type]


def convert_to_flyte_state(state: str) -> State:
    """
    Convert the state from the agent to the state in flyte.
    """
    state = state.lower()
    if state in ["failed"]:
        return RETRYABLE_FAILURE
    elif state in ["done", "succeeded"]:
        return SUCCEEDED
    elif state in ["running"]:
        return RUNNING
    raise ValueError(f"Unrecognized state: {state}")


def is_terminal_state(state: State) -> bool:
    """
    Return true if the state is terminal.
    """
    return state in [SUCCEEDED, RETRYABLE_FAILURE, PERMANENT_FAILURE]


class AsyncAgentExecutorMixin:
    """
    This mixin class is used to run the agent task locally, and it's only used for local execution.
    Task should inherit from this class if the task can be run in the agent.
    """

    is_canceled = False

    def execute(self, **kwargs) -> typing.Any:
        from unittest.mock import MagicMock

        from flytekit.tools.translator import get_serializable

        entity = typing.cast(PythonTask, self)
        m: OrderedDict = OrderedDict()
        dummy_context = MagicMock(spec=grpc.ServicerContext)
        cp_entity = get_serializable(m, settings=SerializationSettings(ImageConfig()), entity=entity)
        agent = AgentRegistry.get_agent(dummy_context, cp_entity.template.type)

        if agent is None:
            raise Exception("Cannot run the task locally, please mock.")
        literals = {}
        ctx = FlyteContext.current_context()
        for k, v in kwargs.items():
            literals[k] = TypeEngine.to_literal(ctx, v, type(v), entity.interface.inputs[k].type)
        inputs = LiteralMap(literals) if literals else None
        output_prefix = ctx.file_access.get_random_local_directory()
        cp_entity = get_serializable(m, settings=SerializationSettings(ImageConfig()), entity=entity)
        if agent.asynchronous:
            loop = asyncio.new_event_loop()
            res = loop.run_until_complete(agent.async_create(dummy_context, output_prefix, cp_entity.template, inputs))
            loop.add_signal_handler(
                signal.SIGINT,
                partial(
                    self.signal_handler,
                    agent=agent,
                    context=dummy_context,
                    resource_meta=res.resource_meta,
                    loop=loop,
                ),
            )
        else:
            res = agent.create(dummy_context, output_prefix, cp_entity.template, inputs)
            signal.signal(signal.SIGINT, partial(self.signal_handler, agent, dummy_context, res.resource_meta, None))
        state = RUNNING
        metadata = res.resource_meta
        progress = Progress(transient=True)
        task = progress.add_task(f"[cyan]Running Task {entity.name}...", total=None)
        with progress:
            while not is_terminal_state(state):
                progress.start_task(task)
                time.sleep(1)
                if agent.asynchronous:
                    res = loop.run_until_complete(agent.async_get(dummy_context, metadata))
                else:
                    res = agent.get(dummy_context, metadata)
                state = res.resource.state
                logger.info(f"Task state: {state}")

        if agent.asynchronous:
            loop.close()
            if self.is_canceled:
                sys.exit(1)

        if state != SUCCEEDED:
            raise Exception(f"Failed to run the task {entity.name}")

        return LiteralMap.from_flyte_idl(res.resource.outputs)

    def signal_handler(
        self,
        agent: AgentBase,
        context: grpc.ServicerContext,
        resource_meta: bytes,
        loop: typing.Optional[AbstractEventLoop] = None,
        signum: typing.Optional[int] = None,
        frame: typing.Optional[FrameType] = None,
    ) -> typing.Any:
        if agent.asynchronous:
            self.is_canceled = True
            loop.create_task(agent.async_delete(context, resource_meta))
        else:
            agent.delete(context, resource_meta)
            sys.exit(1)
