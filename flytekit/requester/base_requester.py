import collections
import inspect
from abc import abstractmethod
from typing import Any, Dict, Optional, TypeVar

import jsonpickle
from typing_extensions import get_type_hints
from flyteidl.admin.agent_pb2 import GetTaskResponse
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin

T = TypeVar("T")
REQUESTER_MODULE = "requester_module"
REQUESTER_NAME = "requester_name"
REQUESTER_CONFIG_PKL = "requester_config_pkl"
INPUTS = "inputs"


class BaseRequester(AsyncAgentExecutorMixin, PythonTask):
    """
    TODO: Write the docstring
    Base class for all requesters. Sensors are tasks that are designed to run forever, and periodically check for some
    condition to be met. When the condition is met, the sensor will complete. Sensors are designed to be run by the
    sensor agent, and not by the Flyte engine.
    """

    def __init__(
        self,
        name: str,
        task_type: str = "requester",
        **kwargs,
    ):

        super().__init__(
            task_type=task_type,
            name=name,
            **kwargs,
        )

    @abstractmethod
    async def do(self, **kwargs) -> GetTaskResponse:
        raise NotImplementedError

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        cfg = {
            REQUESTER_MODULE: type(self).__module__,
            REQUESTER_NAME: type(self).__name__,
        }
        if self._requester_config is not None:
            cfg[REQUESTER_CONFIG_PKL] = jsonpickle.encode(self._requester_config)
        return cfg
