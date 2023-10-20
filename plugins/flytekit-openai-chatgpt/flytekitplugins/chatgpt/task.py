import json
from typing import Any, Dict

import aiohttp
from flyteidl.admin.agent_pb2 import SUCCEEDED, DoTaskResponse, Resource

from flytekit import FlyteContextManager
from flytekit.core.external_api_task import ExternalApiTask
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import get_agent_secret
from flytekit.models.literals import LiteralMap


class ChatGPTTask(ExternalApiTask):
    """
    This is the simplest form of a ChatGPTTask Task, you can define the model and the input you want.
    """

    _openai_organization: str = None
    _chatgpt_conf: Dict[str, Any] = None

    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        if "openai_organization" not in config:
            raise ValueError("The 'openai_organization' configuration variable is required")

        if "chatgpt_conf" not in config:
            raise ValueError("The 'chatgpt_conf' configuration variable is required")

        self._openai_organization = config["openai_organization"]
        self._chatgpt_conf = config["chatgpt_conf"]

        super().__init__(name=name, config=config, return_type=str, **kwargs)

    async def do(
        self,
        message: str = None,
    ) -> DoTaskResponse:
        self._chatgpt_conf["messages"] = [{"role": "user", "content": message}]
        openai_url = "https://api.openai.com/v1/chat/completions"
        data = json.dumps(self._chatgpt_conf)

        async with aiohttp.ClientSession() as session:
            async with session.post(
                url=openai_url, headers=get_header(openai_organization=self._openai_organization), data=data
            ) as resp:
                if resp.status != 200:
                    raise Exception(f"Failed to execute chatgpt job with error: {resp.reason}")
                response = await resp.json()

        message = response["choices"][0]["message"]["content"]

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


def get_header(openai_organization: str):
    return {
        "OpenAI-Organization": openai_organization,
        "Authorization": f"Bearer {get_agent_secret(secret_key='FLYTE_OPENAI_ACCESS_TOKEN')}",
        "content-type": "application/json",
    }
