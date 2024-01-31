from dataclasses import dataclass
from typing import Any, Optional, Type, Union


from flytekit import ImageSpec
from flytekit.configuration import SerializationSettings
from flytekit.core.python_function_task import PythonInstanceTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class BotoConfig(object):
    service: str
    method: str
    config: dict[str, Any]
    region: str


class BotoTask(AsyncAgentExecutorMixin, PythonInstanceTask[BotoConfig]):
    _TASK_TYPE = "sync-boto"

    def __init__(
        self,
        name: str,
        task_config: BotoConfig,
        inputs: Optional[dict[str, Type]] = None,
        container_image: Optional[Union[str, ImageSpec]] = None,
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_config=task_config,
            task_type=self._TASK_TYPE,
            interface=Interface(inputs=inputs, outputs={"result": dict}),
            container_image=container_image,
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> dict[str, Any]:
        return {
            "service": self.task_config.service,
            "config": self.task_config.config,
            "region": self.task_config.region,
            "method": self.task_config.method,
        }
