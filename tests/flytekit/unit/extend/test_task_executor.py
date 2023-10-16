import collections
from unittest.mock import MagicMock

import grpc
import pytest

from flytekit.core.external_api_task import TASK_MODULE, TASK_NAME, ExternalApiTask
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.models import literals
from tests.flytekit.unit.extend.test_agent import get_task_template


class MockExternalApiTask(ExternalApiTask):
    async def do(self, input: str, **kwargs) -> str:
        return input


@pytest.mark.asyncio
async def test_task_executor_engine():
    input = "TASK INPUT"

    interface = Interface(
        inputs=collections.OrderedDict({"input": str, "kwargs": None}),
        outputs=collections.OrderedDict({"o0": str}),
    )
    tmp = get_task_template("api_task")
    tmp._custom = {
        TASK_MODULE: MockExternalApiTask.__module__,
        TASK_NAME: MockExternalApiTask.__name__,
    }

    tmp._interface = transform_interface_to_typed_interface(interface)

    task_inputs = literals.LiteralMap(
        {
            "input": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(string_value="TASK INPUT"))),
        },
    )

    ctx = MagicMock(spec=grpc.ServicerContext)
    agent = AgentRegistry.get_agent("api_task")

    res = await agent.async_do(ctx, tmp, task_inputs)
    assert res == input
