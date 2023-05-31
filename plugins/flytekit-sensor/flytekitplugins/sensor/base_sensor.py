from typing import Any, Dict, Optional, Tuple, Type, TypeVar

from flytekit.core.base_task import PythonTask, TaskMetadata
from flytekit.core.interface import Interface

T = TypeVar("T")


class BaseSensor(PythonTask[T]):
    def __init__(
        self,
        name: str,
        task_config: Optional[T] = None,
        task_type="sensor",
        inputs: Optional[Dict[str, Tuple[Type, Any]]] = None,
        metadata: Optional[TaskMetadata] = None,
        outputs: Optional[Dict[str, Type]] = None,
        **kwargs,
    ):
        super().__init__(
            task_type=task_type,
            name=name,
            interface=Interface(inputs=inputs or {}, outputs=outputs or {}),
            metadata=metadata,
            task_config=task_config,
            **kwargs,
        )

    def execute(self, **kwargs) -> Any:
        raise Exception("Cannot run a sensor natively, please mock.")
