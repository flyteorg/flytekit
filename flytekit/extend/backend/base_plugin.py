import typing
from abc import ABC, abstractmethod

import grpc
from flyteidl.core.tasks_pb2 import TaskTemplate
from flyteidl.service.external_plugin_service_pb2 import (
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


class BackendPluginBase(ABC):
    """
    This is the base class for all backend plugins. It defines the interface that all plugins must implement.
    The external plugins service will be run either locally or in a pod, and will be responsible for
    invoking backend plugins. The propeller will communicate with the external plugins service
    to create tasks, get the status of tasks, and delete tasks.

    All the backend plugins should be registered in the BackendPluginRegistry. External plugins service
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


class BackendPluginRegistry(object):
    """
    This is the registry for all backend plugins. The external plugins service will look up the plugin
    based on the task type.
    """

    _REGISTRY: typing.Dict[str, BackendPluginBase] = {}

    @staticmethod
    def register(plugin: BackendPluginBase):
        if plugin.task_type in BackendPluginRegistry._REGISTRY:
            raise ValueError(f"Duplicate plugin for task type {plugin.task_type}")
        BackendPluginRegistry._REGISTRY[plugin.task_type] = plugin
        logger.info(f"Registering backend plugin for task type {plugin.task_type}")

    @staticmethod
    def get_plugin(context: grpc.ServicerContext, task_type: str) -> typing.Optional[BackendPluginBase]:
        if task_type not in BackendPluginRegistry._REGISTRY:
            logger.error(f"Cannot find backend plugin for task type [{task_type}]")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details(f"Cannot find backend plugin for task type [{task_type}]")
            return None
        return BackendPluginRegistry._REGISTRY[task_type]


def convert_to_flyte_state(state: str) -> State:
    """
    Convert the state from the backend plugin to the state in flyte.
    """
    state = state.lower()
    if state in ["failed"]:
        return RETRYABLE_FAILURE
    elif state in ["done", "succeeded"]:
        return SUCCEEDED
    elif state in ["running"]:
        return RUNNING
    raise ValueError(f"Unrecognized state: {state}")
