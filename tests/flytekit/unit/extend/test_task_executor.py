import collections
from unittest.mock import MagicMock

import grpc
import pytest
from flyteidl.admin.agent_pb2 import SUCCEEDED, DoTaskResponse, Resource

from flytekit import FlyteContext, FlyteContextManager
from flytekit.core.external_api_task import TASK_MODULE, TASK_NAME, TASK_TYPE, ExternalApiTask
from flytekit.core.interface import Interface, transform_interface_to_typed_interface
from flytekit.core.type_engine import TypeEngine
from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.models import literals
from flytekit.models.literals import LiteralMap
from tests.flytekit.unit.extend.test_agent import get_task_template


class MockExternalApiTask(ExternalApiTask):
    async def do(self, input: str, **kwargs) -> DoTaskResponse:
        ctx = FlyteContextManager.current_context()
        outputs = LiteralMap(
            {
                "o0": TypeEngine.to_literal(
                    ctx,
                    input,
                    type(input),
                    TypeEngine.to_literal_type(type(input)),
                )
            }
        ).to_flyte_idl()
        return DoTaskResponse(resource=Resource(state=SUCCEEDED, outputs=outputs, message=input))


@pytest.mark.asyncio
async def test_task_executor_engine():
    input = "TASK INPUT"

    interface = Interface(
        inputs=collections.OrderedDict({"input": str, "kwargs": None}),
        outputs=collections.OrderedDict({"o0": str}),
    )
    tmp = get_task_template(TASK_TYPE, True)
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
    output_prefix = FlyteContext.current_context().file_access.get_random_local_directory()

    ctx = MagicMock(spec=grpc.ServicerContext)
    agent = AgentRegistry.get_agent(TASK_TYPE)

    res = await agent.async_do(ctx, output_prefix, tmp, task_inputs)
    assert res.resource.state == SUCCEEDED
    assert (
        res.resource.outputs
        == literals.LiteralMap(
            {
                "o0": literals.Literal(
                    scalar=literals.Scalar(
                        primitive=literals.Primitive(
                            string_value=input,
                        )
                    )
                )
            }
        ).to_flyte_idl()
    )
    assert res.resource.message == input
