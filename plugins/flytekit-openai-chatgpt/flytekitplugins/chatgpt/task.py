import asyncio
from typing import Any, Dict
from flytekit.configuration import DefaultImages, SerializationSettings

import openai
from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, Resource
from flytekit.core.base_task import PythonTask
from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin, get_agent_secret
from flytekit.models.literals import LiteralMap

import collections
import inspect
from abc import abstractmethod
from typing import Any, Dict, Optional, TypeVar

from flyteidl.admin.agent_pb2 import CreateTaskResponse
from typing_extensions import get_type_hints

from flytekit.configuration import SerializationSettings
from flytekit.core.base_task import PythonTask
from flytekit.core.interface import Interface
from flytekit.extend.backend.base_agent import AsyncAgentExecutorMixin



class ChatGPTTask(AsyncAgentExecutorMixin, PythonTask):
    """
    This is the simplest form of a ChatGPT Task, you can define the model and the input you want.
    Args:
        openai_organization: OpenAI Organization. String can be found here. https://platform.openai.com/docs/api-reference/organization-optional
        chatgpt_conf: ChatGPT job configuration. Config structure can be found here. https://platform.openai.com/docs/api-reference/completions/create
    """

    _TASK_TYPE = "chatgpt"

    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        if "openai_organization" not in config:
            raise ValueError("The 'openai_organization' configuration variable is required")

        if "chatgpt_conf" not in config:
            raise ValueError("The 'chatgpt_conf' configuration variable is required")

        if "model" not in config["chatgpt_conf"]:
            raise ValueError("The 'model' configuration variable in 'chatgpt_conf' is required")

        inputs = collections.OrderedDict({"message": str})
        outputs = collections.OrderedDict({"o0": str})

        super().__init__(
            task_type=self._TASK_TYPE,
            name=name,
            task_config=config,
            interface=Interface(inputs=inputs, outputs=outputs),
            **kwargs,
        )


        # super().__init__(task_type=self._TASK_TYPE, name=name, task_config=config, **kwargs)
    
    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        return {
            "openai_organization": self.task_config["openai_organization"],
            "chatgpt_conf": self.task_config["chatgpt_conf"],
        }

    # async def create(
    #     self,
    #     message: str = None,
    # ) -> CreateTaskResponse:
    #     openai.organization = self._openai_organization
    #     openai.api_key = get_agent_secret(secret_key="FLYTE_OPENAI_ACCESS_TOKEN")

    #     self._chatgpt_conf["messages"] = [{"role": "user", "content": message}]

    #     completion = await asyncio.wait_for(openai.ChatCompletion.acreate(**self._chatgpt_conf), TIMEOUT_SECONDS)
    #     message = completion.choices[0].message.content

    #     ctx = FlyteContextManager.current_context()
    #     outputs = LiteralMap(
    #         {
    #             "o0": TypeEngine.to_literal(
    #                 ctx,
    #                 message,
    #                 type(message),
    #                 TypeEngine.to_literal_type(type(message)),
    #             )
    #         }
    #     ).to_flyte_idl()
    #     return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs))
