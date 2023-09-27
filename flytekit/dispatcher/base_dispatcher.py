import collections
import inspect
from abc import abstractmethod
from typing import Any, Dict, Optional, TypeVar

import jsonpickle
from flyteidl.admin.agent_pb2 import DoTaskResponse
from typing_extensions import get_type_hints

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin

T = TypeVar("T")
DISPATCHER_MODULE = "dispatcher_module"
DISPATCHER_NAME = "dispatcher_name"
DISPATCHER_CONFIG_PKL = "dispatcher_config_pkl"
INPUTS = "inputs"


class BaseDispatcher(AsyncAgentExecutorMixin, PythonTask):
    """
    TODO: Write the docstring
    """

    def __init__(
        self,
        name: str,
        dispatcher_config: Optional[T] = None,
        task_type: str = "dispatcher",
        return_type: Optional[T] = None,
        **kwargs,
    ):
        type_hints = get_type_hints(self.do, include_extras=True)
        signature = inspect.signature(self.do)
        inputs = collections.OrderedDict()
        outputs = collections.OrderedDict({"o0": return_type})

        for k, _ in signature.parameters.items():  # type: ignore
            annotation = type_hints.get(k, None)
            inputs[k] = annotation

        super().__init__(
            task_type=task_type,
            name=name,
            task_config=None,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )
        self._dispatcher_config = dispatcher_config

    @abstractmethod
    async def async_do(self, **kwargs) -> DoTaskResponse:
        raise NotImplementedError

    def get_custom(self, settings: SerializationSettings = None) -> Dict[str, Any]:
        cfg = {
            DISPATCHER_MODULE: type(self).__module__,
            DISPATCHER_NAME: type(self).__name__,
        }
        if self._dispatcher_config is not None:
            cfg[DISPATCHER_CONFIG_PKL] = jsonpickle.encode(self._dispatcher_config)
        return cfg
