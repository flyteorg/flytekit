from datetime import timedelta
from unittest import mock

import pytest
from flyteidl.core.execution_pb2 import TaskExecution

from flytekit.extend.backend.base_agent import AgentRegistry
from flytekit.interfaces.cli_identifiers import Identifier
from flytekit.models import literals
from flytekit.models.core.identifier import ResourceType
from flytekit.models.literals import LiteralMap
from flytekit.models.task import RuntimeMetadata, TaskMetadata, TaskTemplate


async def mock_acreate(*args, **kwargs) -> str:
    mock_response = mock.MagicMock()
    mock_choice = mock.MagicMock()
    mock_choice.message.content = "mocked_message"
    mock_response.choices = [mock_choice]
    return mock_response


@pytest.mark.asyncio
async def test_chatgpt_agent():
    agent = AgentRegistry.get_agent("chatgpt")
    task_id = Identifier(
        resource_type=ResourceType.TASK, project="project", domain="domain", name="name", version="version"
    )
    task_config = {
        "openai_organization": "test-openai-orgnization-id",
        "chatgpt_config": {"model": "gpt-3.5-turbo", "temperature": 0.7},
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
        (),
    )
    tmp = TaskTemplate(
        id=task_id,
        custom=task_config,
        metadata=task_metadata,
        interface=None,
        type="chatgpt",
    )

    task_inputs = LiteralMap(
        {
            "message": literals.Literal(
                scalar=literals.Scalar(primitive=literals.Primitive(string_value="Test ChatGPT Plugin"))
            ),
        },
    )
    message = "mocked_message"
    mocked_token = "mocked_openai_api_key"
    mocked_context = mock.patch("flytekit.current_context", autospec=True).start()
    mocked_context.return_value.secrets.get.return_value = mocked_token

    with mock.patch("openai.resources.chat.completions.AsyncCompletions.create", new=mock_acreate):
        # Directly await the coroutine without using asyncio.run
        response = await agent.do(tmp, task_inputs)

    assert response.phase == TaskExecution.SUCCEEDED
    assert response.outputs == {"o0": message}
