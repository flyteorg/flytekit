import asyncio
from typing import Any, Dict, Optional

import openai
from flyteidl.admin.agent_pb2 import SUCCEEDED, DoTaskResponse, Resource

from flytekit import FlyteContextManager
from flytekit.core.external_api_task import ExternalApiTask
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import get_agent_secret
from flytekit.models.literals import LiteralMap

TIMEOUT_SECONDS = 10


class ChatGPTTask(ExternalApiTask):
    """
    This is the simplest form of a ChatGPTTask Task, you can define the model and the input you want.

    Args:
        openai_organization: OpenAI Organization. String can be found here. https://platform.openai.com/docs/api-reference/organization-optional
        chatgpt_conf: ChatGPT job configuration. Config structure can be found here. https://platform.openai.com/docs/api-reference/completions/create
    """

    _openai_organization: Optional[str] = None
    _chatgpt_conf: Dict[str, Any] = None

    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        if "chatgpt_conf" not in config:
            raise ValueError("The 'chatgpt_conf' configuration variable is required")

        if "model" not in config["chatgpt_conf"]:
            raise ValueError("The 'model' configuration variable in 'chatgpt_conf' is required")

        if "openai_organization" in config:
            self._openai_organization = config["openai_organization"]

        self._chatgpt_conf = config["chatgpt_conf"]

        super().__init__(name=name, config=config, return_type=str, **kwargs)

    async def do(
        self,
        message: str = None,
    ) -> DoTaskResponse:
        openai.organization = self._openai_organization
        openai.api_key = get_agent_secret(secret_key="FLYTE_OPENAI_ACCESS_TOKEN")

        self._chatgpt_conf["messages"] = [{"role": "user", "content": message}]

        completion = await asyncio.wait_for(openai.ChatCompletion.acreate(**self._chatgpt_conf), TIMEOUT_SECONDS)
        message = completion.choices[0].message.content

        ctx = FlyteContextManager.current_context()
        outputs = LiteralMap(
            {
                "o0": TypeEngine.to_literal(
                    ctx,
                    message,
                    type(message),
                    TypeEngine.to_literal_type(type(message)),
                )
            }
        ).to_flyte_idl()
        return DoTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs))
