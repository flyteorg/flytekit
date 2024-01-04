import asyncio
from typing import Optional

import grpc
from flyteidl.admin.agent_pb2 import SUCCEEDED, CreateTaskResponse, Resource

from flytekit import FlyteContextManager, lazy_module
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry, get_agent_secret
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

openai = lazy_module("openai")

TIMEOUT_SECONDS = 10
OPENAI_ACCESS_TOKEN_SECRET = "FLYTE_OPENAI_ACCESS_TOKEN"


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
        ctx = FlyteContextManager.current_context()
        input_python_value = TypeEngine.literal_map_to_kwargs(ctx, inputs, {"message": str})
        message = input_python_value["message"]

        custom = task_template.custom
        custom["chatgpt_conf"]["messages"] = [{"role": "user", "content": message}]
        openai.organization = custom["openai_organization"]
        openai.api_key = get_agent_secret(secret_key=OPENAI_ACCESS_TOKEN_SECRET)
        completion = await asyncio.wait_for(openai.ChatCompletion.acreate(**custom["chatgpt_conf"]), TIMEOUT_SECONDS)
        message = completion.choices[0].message.content

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
