import tempfile
from dataclasses import asdict

import pytest
from flyteidl.core.execution_pb2 import TaskExecution

import flytekit.models.interface as interface_models
from flytekit.extend.backend.base_connector import ConnectorRegistry
from flytekit.models import literals, types
from flytekit.sensor import FileSensor
from flytekit.sensor.base_sensor import SensorMetadata
from tests.flytekit.unit.extend.test_connector import get_task_template


@pytest.mark.asyncio
async def test_sensor_engine():
    file = tempfile.NamedTemporaryFile()
    interfaces = interface_models.TypedInterface(
        {
            "path": interface_models.Variable(types.LiteralType(types.SimpleType.STRING), "description1"),
        },
        {},
    )
    tmp = get_task_template("sensor")
    sensor_metadata = SensorMetadata(
        sensor_module=FileSensor.__module__, sensor_name=FileSensor.__name__, inputs={"path": file.name}
    )
    tmp._custom = asdict(sensor_metadata)
    tmp._interface = interfaces

    task_inputs = literals.LiteralMap(
        {
            "path": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(string_value=file.name))),
        },
    )
    connector = ConnectorRegistry.get_connector("sensor")

    res = await connector.create(tmp, task_inputs)

    assert res == sensor_metadata
    resource = await connector.get(sensor_metadata)
    assert resource.phase == TaskExecution.SUCCEEDED
    res = await connector.delete(sensor_metadata)
    assert res is None
