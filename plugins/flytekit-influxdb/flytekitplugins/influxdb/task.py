from datetime import datetime
from typing import Any, Dict, Optional
from . import Aggregation
from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import SyncAgentExecutorMixin


class InfluxDBTask(SyncAgentExecutorMixin, PythonTask):
    """
    This is the simplest form of a ChatGPT Task, you can define the model and the input you want.
    """

    _TASK_TYPE = "chatgpt"

    def __init__(self, name: str, url: str, org: str, **kwargs):
        """
        Args:
            name: Name of this task, should be unique in the project
            openai_organization: OpenAI Organization. String can be found here. https://platform.openai.com/docs/api-reference/organization-optional
            chatgpt_config: ChatGPT job configuration. Config structure can be found here. https://platform.openai.com/docs/api-reference/completions/create
        """

        task_config = {"url": url, "org": org}

        inputs = {
            "bucket": str,
            "measurement": str,
            "start_time": datetime,
            "end_time": datetime,
            "fields": Optional[list[str]],
            "tag_dict": Optional[dict],
            "period_min": Optional[int],
            "aggregation": Optional[Aggregation],
        }
        outputs = {"o0": str}

        super().__init__(
            task_type=self._TASK_TYPE,
            name=name,
            task_config=task_config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "url": self.task_config["url"],
            "org": self.task_config["org"],
        }
