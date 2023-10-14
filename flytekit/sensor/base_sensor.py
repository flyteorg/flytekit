import collections
import inspect
import typing
from abc import abstractmethod
from typing import Any, Dict, Optional, TypeVar

import jsonpickle
from typing_extensions import get_type_hints

from flytekit import ExecutionParameters, FlyteContext, PythonInstanceTask
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import Task, PythonTask
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.models.literals import LiteralMap

T = TypeVar("T")
SENSOR_MODULE = "sensor_module"
SENSOR_NAME = "sensor_name"
SENSOR_CONFIG_PKL = "sensor_config_pkl"
INPUTS = "inputs"


class BaseSensor(AsyncAgentExecutorMixin, PythonTask):
    """
    Base class for all sensors. Sensors are tasks that are designed to run forever, and periodically check for some
    condition to be met. When the condition is met, the sensor will complete. Sensors are designed to be run by the
    sensor agent, and not by the Flyte engine.
    """

    def __init__(
        self,
        name: str,
        sensor_config: Optional[T] = None,
        task_type: str = "sensor",
        **kwargs,
    ):
        type_hints = get_type_hints(self.poke, include_extras=True)
        signature = inspect.signature(self.poke)
        inputs = collections.OrderedDict()
        for k, v in signature.parameters.items():  # type: ignore
            annotation = type_hints.get(k, None)
            inputs[k] = annotation

        # self._python_interface = Interface(inputs=inputs)

        super().__init__(
            task_type=task_type,
            name=name,
            task_config=None,
            interface=Interface(inputs=inputs),
            **kwargs,
        )
        self._sensor_config = sensor_config

    # @property
    # def python_interface(self) -> Optional[Interface]:
    #     return self._python_interface
    #
    # def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
    #     print("pre_execute")
    #     pass
    #
    # def dispatch_execute(self, ctx: FlyteContext, input_literal_map: LiteralMap) -> LiteralMap:
    #     print("dispatch_execute")
    #     pass

    @abstractmethod
    async def poke(self, **kwargs) -> bool:
        """
        This method should be overridden by the user to implement the actual sensor logic. This method should return
        ``True`` if the sensor condition is met, else ``False``.
        """
        raise NotImplementedError

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        cfg = {
            SENSOR_MODULE: type(self).__module__,
            SENSOR_NAME: type(self).__name__,
        }
        if self._sensor_config is not None:
            cfg[SENSOR_CONFIG_PKL] = jsonpickle.encode(self._sensor_config)
        return cfg
