import importlib
import typing
from typing import Optional

import cloudpickle
import grpc
import jsonpickle
from flyteidl.admin.agent_pb2 import (
    RETRYABLE_FAILURE,
    SUCCEEDED,
    DoTaskResponse,
    Resource,
)

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentBase, AgentRegistry
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.requester.base_requester import INPUTS, REQUESTER_CONFIG_PKL, REQUESTER_MODULE, REQUESTER_NAME

T = typing.TypeVar("T")

class RequesterEngine(AgentBase):
    def __init__(self):
        super().__init__(task_type="requester", asynchronous=True)

    async def async_do(
        self,
        context: grpc.ServicerContext,
        output_prefix: str,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
    ) -> DoTaskResponse:
        python_interface_inputs = {
            name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
        }
        ctx = FlyteContextManager.current_context()
        if inputs:
            native_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, python_interface_inputs)
            task_template.custom[INPUTS] = native_inputs
        meta = task_template.custom

        requester_module = importlib.import_module(name=meta[REQUESTER_MODULE])
        requester_def = getattr(requester_module, meta[REQUESTER_NAME])
        requester_config = jsonpickle.decode(meta[REQUESTER_CONFIG_PKL]) if meta.get(REQUESTER_CONFIG_PKL) else None

        cur_state = (
            SUCCEEDED if await requester_def("requester", config=requester_config).do(**inputs) else RETRYABLE_FAILURE
        )

        return DoTaskResponse(resource=Resource(state=cur_state, outputs=None))


AgentRegistry.register(RequesterEngine())
