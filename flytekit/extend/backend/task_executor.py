import importlib
import typing
from typing import Optional

import grpc
import jsonpickle
from flyteidl.admin.agent_pb2 import DoTaskResponse

from flytekit import FlyteContextManager
from flytekit.core.external_api_task import TASK_CONFIG_PKL, TASK_MODULE, TASK_NAME, TASK_TYPE
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate

T = typing.TypeVar("T")


class TaskExecutor(AgentBase):
    """
    TaskExecutor is an agent responsible for executing external API tasks.

    This class is meant to be subclassed when implementing plugins that require
    an external API to perform the task execution. It provides a routing mechanism
    to direct the task to the appropriate handler based on the task's specifications.
    """

    def __init__(self):
        super().__init__(task_type=TASK_TYPE, asynchronous=True)

    async def async_do(
        self,
        context: grpc.ServicerContext,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> DoTaskResponse:
        python_interface_inputs = {
            name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
        }
        ctx = FlyteContextManager.current_context()
        native_inputs = {}
        if inputs:
            native_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, python_interface_inputs)

        meta = task_template.custom

        task_module = importlib.import_module(name=meta[TASK_MODULE])
        task_def = getattr(task_module, meta[TASK_NAME])
        config = jsonpickle.decode(meta[TASK_CONFIG_PKL]) if meta.get(TASK_CONFIG_PKL) else None
        return await task_def(TASK_TYPE, config=config).do(**native_inputs)


AgentRegistry.register(TaskExecutor())
