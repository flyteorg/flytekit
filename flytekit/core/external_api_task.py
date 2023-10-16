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
TASK_MODULE = "task_module"
TASK_NAME = "task_name"
TASK_CONFIG_PKL = "task_config_pkl"
TASK_TYPE = "api_task"
USE_SYNC_PLUGIN = "use_sync_plugin"  # Indicates that the sync plugin in FlytePropeller should be used to run this task


class ExternalApiTask(AsyncAgentExecutorMixin, PythonTask):
    """
    Base class for all external API tasks. External API tasks are tasks that are designed to run until they receive a
    response from an external service. When the response is received, the task will complete. External API tasks are
    designed to be run by the flyte agent.
    """

    def __init__(
        self,
        name: str,
        config: Optional[T] = None,
        task_type: str = TASK_TYPE,
        return_type: Optional[Any] = None,
        **kwargs,
    ):
        type_hints = get_type_hints(self.do, include_extras=True)
        signature = inspect.signature(self.do)
        inputs = collections.OrderedDict()
        outputs = collections.OrderedDict({"o0": return_type}) if return_type else collections.OrderedDict()

        for k, _ in signature.parameters.items():  # type: ignore
            annotation = type_hints.get(k, None)
            inputs[k] = annotation

        super().__init__(
            task_type=task_type,
            name=name,
            task_config=config,
            interface=Interface(inputs=inputs, outputs=outputs),
            use_sync_plugin=True,
            **kwargs,
        )

        self._config = config

    @abstractmethod
    async def do(self, **kwargs) -> DoTaskResponse:
        """
        Initiate an HTTP request to an external service such as OpenAI or Vertex AI and retrieve the response.
        """
        raise NotImplementedError

    def get_custom(self, settings: SerializationSettings = None) -> Dict[str, Any]:
        cfg = {
            TASK_MODULE: type(self).__module__,
            TASK_NAME: type(self).__name__,
        }

        if self._config is not None:
            cfg[TASK_CONFIG_PKL] = jsonpickle.encode(self._config)

        return cfg
