import json
import pickle
import typing
from dataclasses import dataclass
from typing import Any, Dict, Optional, TypeVar
import os
import aiohttp
import grpc
from flyteidl.admin.agent_pb2 import SUCCEEDED, DoTaskResponse, Resource
from google.protobuf.json_format import MessageToDict
from flytekit.core import utils
import flytekit
from flytekit import FlyteContextManager
from flytekit.configuration import SerializationSettings
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry, convert_to_flyte_state
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.requester.base_requester import BaseRequester
from flytekit.core.type_engine import TypeEngine

T = TypeVar("T")


@dataclass
class ChatGPT(object):

    openai_organization: str = None
    chatgpt_conf: Dict[str, str] = None


class ChatGPTRequester(BaseRequester):
    def __init__(self, name: str, task_config: ChatGPT, **kwargs):
        super().__init__(name=name, task_config=task_config, **kwargs)

    def get_custom(self, settings: SerializationSettings) -> Dict[str, Any]:
        job = super().get_custom()
        if isinstance(self.task_config, ChatGPT):
            job["chatgptConf"] = self.task_config.chatgpt_conf
            job["openaiOrganization"] = self.task_config.openai_organization

        return MessageToDict(job.to_flyte_idl())

    # TODO, Know how to write the input output, maybe like google bigquery
    async def async_do(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> DoTaskResponse:
        custom = task_template.custom
        chatgpt_job = custom["chatgptConf"]
        openai_organization = custom["openaiOrganization"]

        openai_url = "https://api.openai.com/v1/chat/completions"
        data = json.dumps(chatgpt_job)

        async with aiohttp.ClientSession() as session:
            async with session.post(openai_url, headers=get_header(openai_organization), data=data) as resp:
                if resp.status != 200:
                    raise Exception(f"Failed to execute chathpt job with error: {resp.reason}")
                response = await resp.json()

        print("Do Response: ", response)
        message = response.choices[0].message.content
        lt = TypeEngine.to_literal_type(str)
        ctx = FlyteContextManager.current_context()
        message = TypeEngine.to_literal(ctx, message, str, lt).to_flyte_idl()
       
        utils.write_proto_to_file(message, os.path.join(output_prefix, "message"))

        return DoTaskResponse(resource=Resource(state=SUCCEEDED))


def get_header(openai_organization: str):
    token = flytekit.current_context().secrets.get("openai", "access_token")
    return {
        "OpenAI-Organization": openai_organization,
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }
