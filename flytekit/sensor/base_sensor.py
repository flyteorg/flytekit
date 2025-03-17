import collections
import datetime
import inspect
import typing
from abc import abstractmethod
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional, TypeVar, Union

from typing_extensions import Protocol, get_type_hints, runtime_checkable

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_connector import AsyncConnectorExecutorMixin, ResourceMeta


@runtime_checkable
class SensorConfig(Protocol):
    def to_dict(self) -> typing.Dict[str, Any]:
        """
        Serialize the sensor config to a dictionary.
        """
        raise NotImplementedError

    @classmethod
    def from_dict(cls, d: typing.Dict[str, Any]) -> "SensorConfig":
        """
        Deserialize the sensor config from a dictionary.
        """
        raise NotImplementedError


@dataclass
class SensorMetadata(ResourceMeta):
    sensor_module: str
    sensor_name: str
    sensor_config: Optional[dict] = None
    inputs: Optional[dict] = None


T = TypeVar("T", bound=SensorConfig)


class BaseSensor(AsyncConnectorExecutorMixin, PythonTask):
    """
    Base class for all sensors. Sensors are tasks that are designed to run forever and periodically check for some
    condition to be met. When the condition is met, the sensor will complete. Sensors are designed to be run by the
    connector and not by the Flyte engine.
    """

    def __init__(
        self,
        name: str,
        timeout: Optional[Union[datetime.timedelta, int]] = None,
        sensor_config: Optional[T] = None,
        task_type: str = "sensor",
        **kwargs,
    ):
        type_hints = get_type_hints(self.poke, include_extras=True)
        signature = inspect.signature(self.poke)
        inputs = collections.OrderedDict()
        for k, _ in signature.parameters.items():  # type: ignore
            annotation = type_hints.get(k, None)
            inputs[k] = annotation

        # Handle metadata and timeout logic
        metadata = kwargs.pop("metadata", None)
        if metadata is not None and timeout is not None:
            if metadata.timeout is not None:
                raise ValueError("You cannot set both timeout and metadata parameters at the same time in the sensor")
            else:
                metadata.timeout = timeout
        else:
            metadata = TaskMetadata(timeout=timeout) if timeout else TaskMetadata()

        super().__init__(
            task_type=task_type,
            name=name,
            task_config=None,
            interface=Interface(inputs=inputs),
            metadata=metadata,
            **kwargs,
        )
        self._sensor_config = sensor_config

    @abstractmethod
    async def poke(self, **kwargs) -> bool:
        """
        This method should be overridden by the user to implement the actual sensor logic. This method should return
        ``True`` if the sensor condition is met, else ``False``.
        """
        raise NotImplementedError

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        sensor_config = self._sensor_config.to_dict() if self._sensor_config else None
        return asdict(
            SensorMetadata(
                sensor_module=type(self).__module__, sensor_name=type(self).__name__, sensor_config=sensor_config
            )
        )
