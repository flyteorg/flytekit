import threading

import grpc
from flyteidl.service.agent_pb2_grpc import AsyncAgentServiceStub
from flyteidl.admin import agent_pb2
from matplotlib import pyplot as plt
from timeit import default_timer as timer
import tempfile
from unittest.mock import MagicMock

import grpc
import pytest
import cloudpickle
from flyteidl.admin.agent_pb2 import (
    SUCCEEDED,
    DeleteTaskResponse,
)

import flytekit.models.interface as interface_models
from flytekit.extend.backend.base_agent import (
    AgentRegistry,
)
from flytekit.models import literals, types
from flytekit.sensor import FileSensor
from flytekit.sensor.base_sensor import SENSOR_MODULE, SENSOR_NAME
from tests.flytekit.unit.extend.test_agent import get_task_template

channel = grpc.insecure_channel('localhost:8000')
stub = AsyncAgentServiceStub(channel)


interfaces = interface_models.TypedInterface(
        {
            "path": interface_models.Variable(types.LiteralType(types.SimpleType.STRING), "description1"),
        },
        {},
    )
tmp = get_task_template("sensor")
tmp._custom = {
    SENSOR_MODULE: FileSensor.__module__,
    SENSOR_NAME: FileSensor.__name__,
}
print(f"@@@config here is type {type(FileSensor)} val {tmp._custom}")


file = tempfile.NamedTemporaryFile()

tmp._interface = interfaces

task_inputs = literals.LiteralMap(
    {
        "path": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(string_value=file.name))),
    },
)

if __name__ == '__main__':
    try:
        for i in range(1):
            res = stub.CreateTask(request=agent_pb2.CreateTaskRequest(template=tmp.to_flyte_idl(), inputs=task_inputs.to_flyte_idl(), output_prefix="/tmp"))
            res = stub.GetTask(request=agent_pb2.GetTaskRequest(task_type="sensor", resource_meta=res.resource_meta))
            print(res)
    except Exception as e:
        print("error:", e)
