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
REQUESTER_MODULE = "requester_module"
REQUESTER_NAME = "requester_name"
REQUESTER_CONFIG_PKL = "requester_config_pkl"
INPUTS = "inputs"


class BaseRequester(AsyncAgentExecutorMixin, PythonTask):
    """
    TODO: Write the docstring
    """

    def __init__(
        self,
        name: str,
        requester_config: Optional[T] = None,
        task_type: str = "requester",
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
        self._requester_config = requester_config

    @abstractmethod
    async def async_do(self, **kwargs) -> DoTaskResponse:
        raise NotImplementedError

    def get_custom(self, settings: SerializationSettings = None) -> Dict[str, Any]:
        cfg = {
            REQUESTER_MODULE: type(self).__module__,
            REQUESTER_NAME: type(self).__name__,
        }
        if self._requester_config is not None:
            cfg[REQUESTER_CONFIG_PKL] = jsonpickle.encode(self._requester_config)
        return cfg
