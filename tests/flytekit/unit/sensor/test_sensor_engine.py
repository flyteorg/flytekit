import tempfile
from unittest.mock import MagicMock

import cloudpickle
import grpc
import pytest
from flyteidl.admin.agent_pb2 import SUCCEEDED, DeleteTaskResponse

import flytekit.models.interface as interface_models
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.models import literals, types
from flytekit.sensor import FileSensor
from flytekit.sensor.base_sensor import SENSOR_MODULE, SENSOR_NAME, SENSOR_TYPE
from tests.flytekit.unit.extend.test_agent import get_task_template


@pytest.mark.asyncio
async def test_sensor_engine():
    interfaces = interface_models.TypedInterface(
        {
            "path": interface_models.Variable(types.LiteralType(types.SimpleType.STRING), "description1"),
        },
        {},
    )
    tmp = get_task_template(SENSOR_TYPE)
    tmp._custom = {
        SENSOR_MODULE: FileSensor.__module__,
        SENSOR_NAME: FileSensor.__name__,
    }
    file = tempfile.NamedTemporaryFile()

    tmp._interface = interfaces

    task_inputs = literals.LiteralMap(
        {
            "path": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(string_value=file.name))),
        },
    )
    ctx = MagicMock(spec=grpc.ServicerContext)
    agent = AgentRegistry.get_agent(SENSOR_TYPE)

    res = await agent.async_create(ctx, "/tmp", tmp, task_inputs)

    metadata_bytes = cloudpickle.dumps(tmp.custom)
    assert res.resource_meta == metadata_bytes
    res = await agent.async_get(ctx, metadata_bytes)
    assert res.resource.state == SUCCEEDED
    res = await agent.async_delete(ctx, metadata_bytes)
    assert res == DeleteTaskResponse()
