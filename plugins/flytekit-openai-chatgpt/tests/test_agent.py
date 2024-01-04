from unittest import mock
from datetime import timedelta
from flytekit.models import literals
from flytekit.models.task import TaskTemplate, TaskMetadata, RuntimeMetadata
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models.core.identifier import ResourceType

import pytest
from flytekit import FlyteContext, FlyteContextManager
from flyteidl.admin.agent_pb2 import SUCCEEDED
from flytekit.extend.backend.base_agent import AgentRegistry

async def mock_acreate(*args, **kwargs) -> str:
    mock_response = mock.MagicMock()
    mock_choice = mock.MagicMock()
    mock_choice.message.content = "mocked_message"
    mock_response.choices = [mock_choice]
    return mock_response


@pytest.mark.asyncio
async def test_chatgpt_agent():
    ctx = FlyteContextManager.current_context()
    agent = AgentRegistry.get_agent("chatgpt")
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_config = {
                    "openai_organization":"test-openai-orgnization-id",
                    "chatgpt_conf":{
                        "model":"gpt-3.5-turbo",
                        "temperature":0.7
                    }
                }
    task_metadata = TaskMetadata(
        True,
        RuntimeMetadata(RuntimeMetadata.RuntimeType.FLYTE_SDK, "1.0.0", "python"),
        timedelta(days=1),
        literals.RetryStrategy(3),
        True,
        "0.1.1b0",
        "This is deprecated!",
        True,
        "A",
    )
    tmp = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=None,
        type="chatgpt",
    )

    task_inputs = literals.LiteralMap(
        {
            "message": literals.Literal(scalar=literals.Scalar(primitive=literals.Primitive(string_value="Test ChatGPT Plugin"))),
        },
    )
    output_prefix = FlyteContext.current_context().file_access.get_random_local_directory()

    with mock.patch("openai.ChatCompletion.acreate", new=mock_acreate):
        with mock.patch("flytekit.extend.backend.base_agent.get_agent_secret", return_value="mocked_secret"):
            # Directly await the coroutine without using asyncio.run
            response = await agent.async_create(ctx, output_prefix, tmp, task_inputs)

    assert response.HasField("resource")
    assert response.resource.state == SUCCEEDED
    assert response.resource.outputs is not None
