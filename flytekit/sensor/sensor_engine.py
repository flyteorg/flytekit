import importlib
from typing import Optional

from flyteidl.core.execution_pb2 import TaskExecution

from flytekit import FlyteContextManager
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry, AsyncAgentBase, Resource
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.sensor.base_sensor import SensorMetadata


class SensorEngine(AsyncAgentBase):
    name = "Sensor"

    def __init__(self):
        super().__init__(task_type_name="sensor", metadata_type=SensorMetadata)

    async def create(self, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None, **kwarg) -> SensorMetadata:
        sensor_metadata = SensorMetadata(**task_template.custom)

        if inputs:
            ctx = FlyteContextManager.current_context()
            python_interface_inputs = {
                name: TypeEngine.guess_python_type(lt.type) for name, lt in task_template.interface.inputs.items()
            }
            native_inputs = await TypeEngine._literal_map_to_kwargs(ctx, inputs, python_interface_inputs)
            sensor_metadata.inputs = native_inputs

        return sensor_metadata

    async def get(self, resource_meta: SensorMetadata, **kwargs) -> Resource:
        sensor_module = importlib.import_module(name=resource_meta.sensor_module)
        sensor_def = getattr(sensor_module, resource_meta.sensor_name)

        inputs = resource_meta.inputs
        cur_phase = (
            TaskExecution.SUCCEEDED
            if await sensor_def("sensor", config=resource_meta.sensor_config).poke(**inputs)
            else TaskExecution.RUNNING
        )
        return Resource(phase=cur_phase, outputs=None)

    async def delete(self, resource_meta: SensorMetadata, **kwargs):
        return


AgentRegistry.register(SensorEngine())
