from abc import abstractmethod
from typing import Any, Dict, Generic, Optional, TypeVar, cast, Tuple, Type

import jsonpickle

from flytekit import ExecutionParameters, FlyteContext, FlyteContextManager, logger
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import Task
from flytekit.core.context_manager import ExecutionState
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.core.tracker import TrackedInstance
from flytekit.core.type_engine import TypeEngine
from flytekit.core.utils import timeit
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.models import literals as _literal_models

T = TypeVar("T")


class BaseSensor(AsyncAgentExecutorMixin, TrackedInstance, Task, Generic[T]):
    """
    Base class for all sensors. Sensors are tasks that are designed to run forever, and periodically check for some
    condition to be met. When the condition is met, the sensor will complete. Sensors are designed to be run by the
    sensor agent, and not by the Flyte engine.
    """

    def __init__(
        self,
        task_type: str,
        name: str,
        sensor_config: Optional[T] = None,
        inputs: Optional[Dict[str, Tuple[Type, Any]]] = None,
        **kwargs,
    ):
        self._python_interface = Interface(inputs=inputs)
        super().__init__(
            task_type=task_type,
            name=name,
            interface=transform_interface_to_typed_interface(self._python_interface),
            **kwargs,
        )
        self._sensor_config = sensor_config

    def python_interface(self) -> Optional[Interface]:
        return self._python_interface

    @abstractmethod
    async def poke(self, config: T) -> bool:
        """
        This method should be overridden by the user to implement the actual sensor logic. This method should return
        ``True`` if the sensor condition is met, else ``False``.
        """
        raise NotImplementedError

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        cfg = {
            "sensor_module": type(self).__module__,
            "sensor_name": type(self).__name__,
        }
        if self._sensor_config is not None:
            cfg["sensor_config_pkl"] = jsonpickle.encode(self._sensor_config)
        return cfg

    def pre_execute(self, user_params: ExecutionParameters) -> ExecutionParameters:
        return user_params

    def dispatch_execute(
        self, ctx: FlyteContext, input_literal_map: _literal_models.LiteralMap
    ) -> _literal_models.LiteralMap:
        # Invoked before the task is executed
        new_user_params = self.pre_execute(ctx.user_space_params)

        # Create another execution context with the new user params, but let's keep the same working dir
        with FlyteContextManager.with_context(
            ctx.with_execution_state(
                cast(ExecutionState, ctx.execution_state).with_params(user_space_params=new_user_params)
            )
            # type: ignore
        ) as exec_ctx:
            try:
                native_inputs = TypeEngine.literal_map_to_kwargs(
                    exec_ctx, input_literal_map, self.python_interface.inputs
                )
            except Exception as exc:
                msg = f"Failed to convert inputs of task '{self.name}':\n  {exc}"
                logger.error(msg)
                raise type(exc)(msg) from exc

            logger.info(f"Invoking {self.name} with inputs: {native_inputs}")
            try:
                with timeit("Execute user level code"):
                    native_outputs = self.execute(**native_inputs)
            except Exception as e:
                logger.exception(f"Exception when executing {e}")
                raise e
            return native_outputs
