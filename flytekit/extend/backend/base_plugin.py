import typing
from abc import ABC, abstractmethod

import grpc
from flyteidl.core.tasks_pb2 import TaskTemplate
from flyteidl.service.agent_service_pb2 import (
    RETRYABLE_FAILURE,
    RUNNING,
    SUCCEEDED,
    State,
    TaskCreateResponse,
    TaskDeleteResponse,
    TaskGetResponse,
)

from flytekit import logger
from flytekit.models.literals import LiteralMap


class AgentBase(ABC):
    """
    This is the base class for all backend plugins. It defines the interface that all plugins must implement.
    The agent service will be run either locally or in a pod, and will be responsible for
    invoking agents. The propeller will communicate with the agent service
    to create tasks, get the status of tasks, and delete tasks.

    All the agents should be registered in the AgentRegistry. Agent Service
    will look up the plugin based on the task type. Every task type can only have one plugin.
    """

    def __init__(self, task_type: str):
        self._task_type = task_type

    @property
    def task_type(self) -> str:
        """
        task_type is the name of the task type that this plugin supports.
        """
        return self._task_type

    @abstractmethod
    def create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: typing.Optional[LiteralMap] = None,
    ) -> TaskCreateResponse:
        """
        Return a Unique ID for the task that was created. It should return error code if the task creation failed.
        """
        pass

    @abstractmethod
    def get(self, context: grpc.ServicerContext, job_id: str) -> TaskGetResponse:
        """
        Return the status of the task, and return the outputs in some cases. For example, bigquery job
        can't write the structured dataset to the output location, so it returns the output literals to the propeller,
        and the propeller will write the structured dataset to the blob store.
        """
        pass

    @abstractmethod
    def delete(self, context: grpc.ServicerContext, job_id: str) -> TaskDeleteResponse:
        """
        Delete the task. This call should be idempotent.
        """
        pass


class AgentRegistry(object):
    """
    This is the registry for all agents. The agent service will look up the agent
    based on the task type.
    """

    _REGISTRY: typing.Dict[str, AgentBase] = {}

    @staticmethod
    def register(plugin: AgentBase):
        if plugin.task_type in AgentRegistry._REGISTRY:
            raise ValueError(f"Duplicate agent for task type {plugin.task_type}")
        AgentRegistry._REGISTRY[plugin.task_type] = plugin
        logger.info(f"Registering an agent for task type {plugin.task_type}")

    @staticmethod
    def get_plugin(context: grpc.ServicerContext, task_type: str) -> typing.Optional[AgentBase]:
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
