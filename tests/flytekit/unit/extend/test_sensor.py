import asyncio
from typing import Optional
from unittest.mock import MagicMock

import cloudpickle
import grpc
from flyteidl.admin.agent_pb2 import DeleteTaskResponse

from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.extend.backend.base_sensor import SensorBase
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate


def test_sensor():
    custom_data = {"path": "/tmp/123"}

    class TestSensor(SensorBase):
        def __init__(self):
            super().__init__(task_type="test_sensor")

        async def poke(self, path: str) -> bool:
            return True

        async def extract(
            self, context: grpc.ServicerContext, task_template: TaskTemplate, inputs: Optional[LiteralMap] = None
        ) -> dict:
            return custom_data

    agent = TestSensor()
    AgentRegistry.register(agent)

    dummy_context = MagicMock(spec=grpc.ServicerContext)
    custom_data = {"path": "/tmp/123"}
    dummy_template = TaskTemplate(
        id="task_id",
        metadata=None,
        interface=None,
        type="dummy",
        custom=custom_data,
    )
    res = asyncio.run(agent.async_create(dummy_context, "/tmp", dummy_template, None))
    assert res.resource_meta == cloudpickle.dumps(custom_data)
    resource_meta = res.resource_meta
    res = asyncio.run(agent.async_get(dummy_context, resource_meta))
    assert res.resource.state == 4
    res = asyncio.run(agent.async_delete(dummy_context, resource_meta))
    assert res == DeleteTaskResponse()
