import importlib
import typing
from typing import Optional

import grpc
import jsonpickle
from flyteidl.admin.agent_pb2 import DoTaskResponse

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, DispatcherAgent
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.dispatcher.base_dispatcher import INPUTS, DISPATCHER_CONFIG_PKL, DISPATCHER_MODULE, DISPATCHER_NAME

T = typing.TypeVar("T")


class DispatcherEngine(DispatcherAgent):
    async def async_do(
        self,
        context: grpc.ServicerContext,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        output_prefix: Optional[str] = None,
    ) -> DoTaskResponse:
        python_interface_inputs = {
            name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
        }
        ctx = FlyteContextManager.current_context()
        if inputs:
            native_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, python_interface_inputs)
            task_template.custom[INPUTS] = native_inputs
        else:
            raise ValueError("Dispatcher needs a input!")

        meta = task_template.custom

        dispatcher_module = importlib.import_module(name=meta[DISPATCHER_MODULE])
        dispatcher_def = getattr(dispatcher_module, meta[DISPATCHER_NAME])
        dispatcher_config = jsonpickle.decode(meta[DISPATCHER_CONFIG_PKL]) if meta.get(DISPATCHER_CONFIG_PKL) else None
        inputs = meta.get(INPUTS, {})
        return await dispatcher_def("dispatcher", config=dispatcher_config).do(**inputs)


AgentRegistry.register(DispatcherEngine())
