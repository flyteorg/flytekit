import importlib
import typing
from typing import Optional

import grpc
import jsonpickle
from flyteidl.admin.agent_pb2 import DoTaskResponse

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, RequesterAgent
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.requester.base_requester import INPUTS, REQUESTER_CONFIG_PKL, REQUESTER_MODULE, REQUESTER_NAME

T = typing.TypeVar("T")


class RequesterEngine(RequesterAgent):
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
        else:
            raise ValueError("Requester needs a input!")

        meta = task_template.custom

        requester_module = importlib.import_module(name=meta[REQUESTER_MODULE])
        requester_def = getattr(requester_module, meta[REQUESTER_NAME])
        requester_config = jsonpickle.decode(meta[REQUESTER_CONFIG_PKL]) if meta.get(REQUESTER_CONFIG_PKL) else None
        inputs = meta.get(INPUTS, {})
        return await requester_def("requester", config=requester_config).do(output_prefix=output_prefix, **inputs)


AgentRegistry.register(RequesterEngine())
