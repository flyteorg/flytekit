import http
import json
import pickle
import typing
from dataclasses import dataclass
from typing import Optional
import asyncio
import openai
from flytekit.core.type_engine import TypeEngine

import grpc
from flyteidl.admin.agent_pb2 import SUCCEEDED
from flyteidl.admin.agent_pb2 import PENDING, CreateTaskResponse, DeleteTaskResponse, GetTaskResponse, Resource
from flytekit import FlyteContextManager
from flytekit import lazy_module
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry, convert_to_flyte_state, get_agent_secret
from flytekit.models.core.execution import TaskLog
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

TIMEOUT_SECONDS = 10

class ChatGPTAgent(AgentBase):
    def __init__(self):
        super().__init__(task_type="chatgpt")
    
    async def async_create(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> CreateTaskResponse:
        message = task_template.interface.inputs
        print("@@@ message:", message)
        config = task_template.custom
        config["chatgpt_conf"]["messages"] = [{"role": "user", "content": message}]
        message = "return value"
        openai.organization = config["openai_organization"]
        openai.api_key = get_agent_secret(secret_key="FLYTE_OPENAI_ACCESS_TOKEN")
        # completion = await asyncio.wait_for(openai.ChatCompletion.acreate(**config["chatgpt_conf"]), TIMEOUT_SECONDS)

        # message = completion.choices[0].message.content

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
        return CreateTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs))


AgentRegistry.register(ChatGPTAgent())
