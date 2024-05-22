from typing import Any, Optional

import jsonpickle
from flyteidl.core.execution_pb2 import TaskExecution
from flytekitplugins.langchain.task import _get_langchain_instance

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, Resource, SyncAgentBase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


class LangChainAgent(SyncAgentBase):
    """
    TODO: Add LangChainAgent description
    """

    name = "LangChain Agent"

    def __init__(self):
        super().__init__(task_type_name="langchain")

    async def do(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> Resource:
        langchain_obj = jsonpickle.decode(task_template.custom["task_config_pkl"])
        langchain_instance = _get_langchain_instance(langchain_obj)
        ctx = FlyteContextManager.current_context()
        
        # We want to support both string and pickle inputs
        # try:
        input_python_value = TypeEngine.literal_map_to_kwargs(ctx, inputs, {"input": Any})
        # except:
        #     input_python_value = TypeEngine.literal_map_to_kwargs(ctx, inputs, {"input": str})

        message = input_python_value["input"]
        message = langchain_instance.invoke(message)

        return Resource(
            phase=TaskExecution.SUCCEEDED,
            outputs={"o0": message},
        )


AgentRegistry.register(LangChainAgent())
