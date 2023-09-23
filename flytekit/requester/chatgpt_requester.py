import json
import os
from typing import Any, Dict

import aiohttp
from flyteidl.admin.agent_pb2 import SUCCEEDED, DoTaskResponse, Resource

import flytekit
from flytekit import FlyteContextManager
from flytekit.core import utils
from flytekit.core.type_engine import TypeEngine
from flytekit.models.literals import LiteralMap
from flytekit.requester.base_requester import BaseRequester


class ChatGPTRequester(BaseRequester):
    """
    TODO: Write the docstring
    """

    _openai_organization: str = None
    _chatgpt_conf: Dict[str, Any] = None

    # TODO,  such as Value Error
    def __init__(self, name: str, config: Dict[str, Any], **kwargs):
        super().__init__(name=name, requester_config=config, **kwargs)
        self._openai_organization = config["openai_organization"]
        self._chatgpt_conf = config["chatgpt_conf"]

    async def do(
        self,
        output_prefix: str = None,
        message: str = None,
    ) -> DoTaskResponse:
        self._chatgpt_conf["messages"] = [{"role": "user", "content": message}]
        openai_url = "https://api.openai.com/v1/chat/completions"
        data = json.dumps(self._chatgpt_conf)

        async with aiohttp.ClientSession() as session:
            async with session.post(
                openai_url, headers=get_header(openai_organization=self._openai_organization), data=data
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

        output_filename = os.path.join(output_prefix, "do.proto")
        utils.write_proto_to_file(outputs, output_filename)

        return DoTaskResponse(resource=Resource(state=SUCCEEDED))


def get_header(openai_organization: str):
    token = flytekit.current_context().secrets.get("openai", "access_token")
    return {
        "OpenAI-Organization": openai_organization,
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
