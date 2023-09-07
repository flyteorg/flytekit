import json
import pickle
import typing
from dataclasses import dataclass
from typing import Optional

import aiohttp
import grpc
from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource

import flytekit
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry, convert_to_flyte_state
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class ChatGPTAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="chatgpt")


    async def async_do(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> GetTaskResponse:
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

        return GetTaskResponse(resource=Resource(state=SUCCEEDED))
        


def get_header(openai_organization: str):
    token = flytekit.current_context().secrets.get("openai", "token")
    return {
        'OpenAI-Organization': openai_organization,
        'Authorization': f'Bearer {token}',
        'content-type': 'application/json'
    }