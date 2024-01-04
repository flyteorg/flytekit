import collections
from dataclasses import dataclass
from typing import Any, Dict

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin


@dataclass
class ChatGPTConfig(object):
    """
    ChatGPTConfig should be used to configure a ChatGPT Task.

    Args:
        openai_organization: OpenAI Organization. String can be found here. https://platform.openai.com/docs/api-reference/organization-optional
        chatgpt_conf: ChatGPT job configuration. Config structure can be found here. https://platform.openai.com/docs/api-reference/completions/create
    """

    openai_organization: str
    chatgpt_config: Dict[str, Any]


class ChatGPTTask(AsyncAgentExecutorMixin, PythonTask):
    """
    This is the simplest form of a ChatGPT Task, you can define the model and the input you want.
    """

    _TASK_TYPE = "chatgpt"

    def __init__(self, name: str, config: ChatGPTConfig, **kwargs):
        """
        Args:
            name: Name of this task, should be unique in the project
            config: ChatGPT Config
        """

        if "model" not in config.chatgpt_config:
            raise ValueError("The 'model' configuration variable is required in chatgpt_config")

        inputs = collections.OrderedDict({"message": str})
        outputs = collections.OrderedDict({"o0": str})

        super().__init__(
            task_type=self._TASK_TYPE,
            name=name,
            task_config=config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "openai_organization": self.task_config.openai_organization,
            "chatgpt_config": self.task_config.chatgpt_config,
        }
