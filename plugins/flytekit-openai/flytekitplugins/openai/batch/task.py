from typing import Any, Dict

from flytekit import kwtypes
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin
from flytekit.types.file import FlyteFile


class BatchEndpointTask(AsyncAgentExecutorMixin, PythonTask):
    _TASK_TYPE = "batch-endpoint"

    def __init__(
        self,
        name: str,
        openai_organization: str,
        config: Dict[str, Any],
        **kwargs,
    ):
        super().__init__(
            name=name,
            task_type=self._TASK_TYPE,
            interface=Interface(
                inputs=kwtypes(input_file=FlyteFile, metadata=dict),
                outputs=kwtypes(output_file_id=str, metadata=dict),
            ),
            **kwargs,
        )

        self._openai_organization = openai_organization
        self._config = config

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "openai_organization": self._openai_organization,
            "config": self._config,
        }
