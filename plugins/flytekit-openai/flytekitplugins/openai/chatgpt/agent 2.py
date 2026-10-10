import asyncio
import logging
from typing import Optional

from flyteidl.core.execution_pb2 import TaskExecution

from flytekit import FlyteContextManager, lazy_module
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, Resource, SyncAgentBase
from flytekit.extend.backend.utils import get_agent_secret
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

openai = lazy_module("openai")

TIMEOUT_SECONDS = 10
OPENAI_API_KEY = "FLYTE_OPENAI_API_KEY"


class ChatGPTAgent(SyncAgentBase):
    name = "ChatGPT Agent"

    def __init__(self):
        super().__init__(task_type_name="chatgpt")

    async def do(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> Resource:
        ctx = FlyteContextManager.current_context()
        input_python_value = TypeEngine.literal_map_to_kwargs(ctx, inputs, {"message": str})
        message = input_python_value["message"]

        custom = task_template.custom
        custom["chatgpt_config"]["messages"] = [{"role": "user", "content": message}]
        client = openai.AsyncOpenAI(
            organization=custom["openai_organization"],
            api_key=get_agent_secret(secret_key=OPENAI_API_KEY),
        )

        logger = logging.getLogger("httpx")
        logger.setLevel(logging.WARNING)

        completion = await asyncio.wait_for(client.chat.completions.create(**custom["chatgpt_config"]), TIMEOUT_SECONDS)
        message = completion.choices[0].message.content
        outputs = {"o0": message}

        return Resource(phase=TaskExecution.SUCCEEDED, outputs=outputs)


AgentRegistry.register(ChatGPTAgent())
